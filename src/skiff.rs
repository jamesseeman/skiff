pub mod raft_proto {
    tonic::include_proto!("raft");
}

pub mod skiff_proto {
    tonic::include_proto!("skiff");
}

use crate::error::SkiffError;
use raft_proto::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
    EntryReply, EntryRequest, ServerReply, ServerRequest, VoteReply, VoteRequest,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use skiff_proto::{
    skiff_server::SkiffServer, DeleteReply, DeleteRequest, GetReply, GetRequest, InsertReply,
    InsertRequest,
};
use std::path::Path;
use std::time::Duration;
use std::{cmp::min, fs};
use std::{collections::HashMap, str::FromStr};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex, Notify};
use tonic::{transport::Channel, Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Action {
    Insert(String, Vec<u8>),
    Delete(String),
    Configure(Vec<(String, Ipv4Addr)>),
}

#[derive(Debug, Clone)]
struct Log {
    index: u32,
    term: u32,
    action: Action,
    committed: Arc<(Mutex<bool>, Notify)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum ElectionState {
    Candidate,
    Leader,
    Follower,
}

#[derive(Debug, Clone)]
struct State {
    election_state: ElectionState,
    current_term: u32,
    voted_for: Option<Uuid>,
    committed_index: u32,
    last_applied: u32,

    // todo: potentically separate cluster logic
    peer_clients: HashMap<Uuid, Arc<Mutex<RaftClient<Channel>>>>,

    // todo: potentially separate leader state
    next_index: HashMap<Uuid, u32>,
    match_index: HashMap<Uuid, u32>,

    log: Vec<Log>,
    conn: sled::Tree,
}

#[derive(Debug, Clone)]
pub struct Skiff {
    id: Uuid,
    address: Ipv4Addr,
    state: Arc<Mutex<State>>,
    tx_entries: Arc<Mutex<mpsc::Sender<u8>>>,
    rx_entries: Arc<Mutex<mpsc::Receiver<u8>>>,
}

impl Skiff {
    pub fn new(
        id: Uuid,
        address: Ipv4Addr,
        data_dir: String,
        peers: Vec<Ipv4Addr>,
    ) -> Result<Self, SkiffError> {
        let conn = sled::open(data_dir)?.open_tree(id.to_string())?;
        let (tx_entries, rx_entries) = mpsc::channel(32);

        // First convert peers into a vector of tuples (Uuid, Ipv4Addr)
        // We don't know the actual uuids, we will retrieve them once the server starts
        // For now, intialize with nil Uuids
        // Finally add this node to the cluster
        let mut cluster: Vec<(String, Ipv4Addr)> = peers
            .into_iter()
            .map(|addr| (Uuid::nil().to_string(), addr))
            .collect();

        cluster.push((id.to_string(), address));

        // Initialize the log with the cluster
        // Todo: need to consider how this will work once snapshots are implemented, as we'll need
        // to initialize log with old config
        // Todo: at the moment, with committed == last_applied == 0, this first log won't
        // ever get written to disk. Needs to be resolved. Could be fixed with something like
        // last_applied = -1, would require changing last_applied to i64
        Ok(Skiff {
            id,
            address,
            state: Arc::new(Mutex::new(State {
                election_state: ElectionState::Follower,
                current_term: 0,
                voted_for: None,
                committed_index: 0,
                last_applied: 0,
                peer_clients: HashMap::new(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                log: vec![Log {
                    term: 0,
                    index: 0,
                    action: Action::Configure(cluster),
                    committed: Arc::new((Mutex::new(true), Notify::new())),
                }],
                conn,
            })),
            tx_entries: Arc::new(Mutex::new(tx_entries)),
            rx_entries: Arc::new(Mutex::new(rx_entries)),
        })
    }

    // todo: initializing a new cluster with a known config without needing to send add_server rpc

    // todo: something like
    // fn from_id(id: Uuid, data_dir) -> Result<Self, SkifError> {

    // Todo: consider making these macros
    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn get_address(&self) -> Ipv4Addr {
        self.address
    }

    // Todo: consider if this should return result, although there should always be a cluster_config log
    // handle missing cluster configuration? sole node in cluster?
    async fn get_cluster(&self) -> Result<Vec<(Uuid, Ipv4Addr)>, SkiffError> {
        let config = match self
            .state
            .lock()
            .await
            .log
            .iter()
            .rev()
            .find(|log| matches!(log.action, Action::Configure(_)))
        {
            Some(log) => match &log.action {
                Action::Configure(config) => config.clone(),
                _ => return Err(SkiffError::MissingClusterConfig),
            },
            _ => return Err(SkiffError::MissingClusterConfig),
        };

        Ok(config
            .into_iter()
            .map(|(id, address)| (Uuid::from_str(&id).unwrap(), address))
            .collect())
    }

    async fn get_peers(&self) -> Vec<(Uuid, Ipv4Addr)> {
        self.get_cluster()
            .await
            // Todo: figure out if unwrap is sufficient or if this needs to return Result
            // affects get_peer_clients()
            .unwrap()
            .into_iter()
            .filter(|(_, addr)| *addr != self.address)
            .collect()
    }

    async fn get_peer_clients(&self) -> Vec<(Uuid, Arc<Mutex<RaftClient<Channel>>>)> {
        let peers = self.get_peers().await;

        let lock = self.state.lock().await;
        let mut retry = vec![];
        let mut peer_clients: Vec<(Uuid, Arc<Mutex<RaftClient<Channel>>>)> = peers
            .into_iter()
            .filter_map(
                |(peer_id, peer_addr)| match lock.peer_clients.get(&peer_id) {
                    Some(client) => Some((peer_id, client.clone())),
                    None => {
                        retry.push((peer_id, peer_addr));
                        None
                    }
                },
            )
            .collect();

        // todo: bit sloppy, try to fix
        drop(lock);
        let mut lock = self.state.lock().await;

        for (peer_id, peer_addr) in retry {
            match RaftClient::connect(format!("http://{}", SocketAddrV4::new(peer_addr, 9400)))
                .await
            {
                Ok(client) => {
                    let arc = Arc::new(Mutex::new(client));
                    lock.peer_clients.insert(peer_id, arc.clone());
                    peer_clients.push((peer_id, arc));
                }
                Err(_) => {
                    println!("Failed to connect");
                }
            }
        }

        peer_clients
    }

    async fn drop_peer_client(&self, id: &Uuid) {
        let mut lock = self.state.lock().await;
        let _ = lock.peer_clients.remove(id);
    }

    async fn get_election_state(&self) -> ElectionState {
        self.state.lock().await.election_state.clone()
    }

    async fn set_election_state(&self, state: ElectionState) {
        self.state.lock().await.election_state = state;
    }

    async fn get_current_term(&self) -> u32 {
        self.state.lock().await.current_term
    }

    async fn increment_term(&self) {
        self.state.lock().await.current_term += 1;
    }

    async fn set_current_term(&self, term: u32) {
        self.state.lock().await.current_term = term;
    }

    // todo: this is a slightly naive approach, retrieving index for log[-1], might not be 100% accurate
    // should be, since index increases monotonically, but might not in the event of cluster issues?
    async fn get_last_log_index(&self) -> u32 {
        self.state
            .lock()
            .await
            .log
            .last()
            .map(|last| last.index)
            .unwrap_or(0)
    }

    async fn get_last_log_term(&self) -> u32 {
        self.state
            .lock()
            .await
            .log
            .last()
            .map(|last| last.term)
            .unwrap_or(0)
    }

    async fn get_commit_index(&self) -> u32 {
        self.state.lock().await.committed_index
    }

    async fn vote_for(&self, candidate_id: Option<Uuid>) {
        self.state.lock().await.voted_for = candidate_id;
    }

    async fn get_voted_for(&self) -> Option<Uuid> {
        self.state.lock().await.voted_for
    }

    async fn log(&self, action: Action) -> Arc<(Mutex<bool>, Notify)> {
        let mut lock = self.state.lock().await;
        let current_term = lock.current_term;
        let last_index = lock.log.last().map(|last: &Log| last.index).unwrap_or(0);

        let commit_pair = Arc::new((Mutex::new(false), Notify::new()));
        lock.log.push(Log {
            index: last_index + 1,
            term: current_term,
            action,
            committed: commit_pair.clone(),
        });

        commit_pair
    }

    async fn get_logs(&self, peer: &Uuid) -> (u32, u32, Vec<raft_proto::Log>) {
        let lock = self.state.lock().await;
        let log_next_index = lock.next_index.get(peer).unwrap();
        let mut prev_log_index = 0;
        let mut prev_log_term = 0;
        let mut new_logs: Vec<raft_proto::Log> = vec![];

        for log in &lock.log {
            // Get latest log that should already be in peer's log
            if log.index < *log_next_index && log.index > prev_log_index {
                prev_log_index = log.index;
                prev_log_term = log.term;
            } else if log.index >= *log_next_index {
                new_logs.push(match &log.action {
                    Action::Insert(key, value) => raft_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: raft_proto::Action::Insert as i32,
                        key: key.clone(),
                        value: Some(value.clone()),
                    },
                    Action::Delete(key) => raft_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: raft_proto::Action::Delete as i32,
                        key: key.clone(),
                        value: None,
                    },
                    Action::Configure(config) => raft_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: raft_proto::Action::Configure as i32,
                        // Todo: obviously can collide with user "cluster" key, need to delineate
                        // The best thing to do would likely be to store in separate tree
                        // The tree would also be delineated from user k/v space
                        key: "cluster".to_string(),
                        value: Some(bincode::serialize(&config).unwrap()),
                    },
                });
            }
        }

        println!("len: {}", new_logs.len());

        (prev_log_index, prev_log_term, new_logs)
    }

    async fn commit_logs(&self) -> Result<(), sled::Error> {
        let committed_index = self.state.lock().await.committed_index;
        let last_applied = self.state.lock().await.last_applied;
        if committed_index <= last_applied {
            return Ok(());
        }

        println!("Committing");
        let new_logs: Vec<(usize, Action)> = self
            .state
            .lock()
            .await
            .log
            .iter()
            .enumerate()
            .filter(|(_, log)| log.index > last_applied)
            .map(|(i, log)| (i, log.action.clone()))
            .collect();

        for (i, action) in new_logs {
            match action {
                Action::Insert(key, value) => {
                    self.state.lock().await.conn.insert(key, value)?;
                }
                Action::Delete(key) => {
                    self.state.lock().await.conn.remove(key)?;
                }
                Action::Configure(config) => {
                    self.state
                        .lock()
                        .await
                        .conn
                        .insert("cluster", bincode::serialize(&config).unwrap())?;
                }
            }

            if let Some(log) = self.state.lock().await.log.get_mut(i) {
                let (committed, notify) = &*log.committed;
                *committed.lock().await = true;
                notify.notify_one();
            }
        }

        self.state.lock().await.last_applied = committed_index;

        Ok(())
    }

    async fn reset_heartbeat_timer(&self) {
        let _ = self.tx_entries.lock().await.send(1).await; // Can be anything
    }

    // Todo: Add token for authenticated joins
    // Todo: snapshot / log compaction, resuming operation after restart
    pub async fn start(self) -> Result<(), anyhow::Error> {
        // check if we're joining an existing cluster
        if self.get_cluster().await.unwrap().len() > 1 {
            println!("Joining cluster");

            let mut joined_cluster = false;
            for (id, client) in self.get_peer_clients().await {
                println!("Asking {:?} to join cluster", id);
                
                let mut request = Request::new(ServerRequest {
                    id: self.id.to_string(),
                    address: self.address.to_string(),
                });

                request.set_timeout(Duration::from_millis(300));

                match client.lock().await.add_server(request).await {
                    Ok(response) => {
                        let inner = response.into_inner();
                        if inner.success {
                            if let Some(cluster) = inner.cluster {
                                // Todo: this will get logged twice. Once here and once from append_entry
                                // It shouldn't be an issue, but this is an unecessary duplication
                                self.log(Action::Configure(
                                    bincode::deserialize(&cluster).unwrap(),
                                ))
                                .await;

                                joined_cluster = true;

                                break
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }

            if !joined_cluster {
                return Err(SkiffError::ClusterJoinFailed.into());
            }
        }

        let mut server1 = self.clone();
        let _elections: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
            tokio::spawn(async move {
                server1.election_manager().await?;
                Ok(())
            });

        let bind_address = SocketAddr::new(self.get_address().into(), 9400);
        tonic::transport::Server::builder()
            .add_service(RaftServer::new(self.clone()))
            .add_service(SkiffServer::new(self))
            .serve(bind_address)
            .await?;

        Ok(())
    }

    // todo: rename or break up, as it's handling leader logic as well as elections
    async fn election_manager(&mut self) -> Result<(), anyhow::Error> {
        let mut rng = {
            let rng = rand::thread_rng();
            StdRng::from_rng(rng)?
        };

        let mut rx_entries = self.rx_entries.lock().await;

        #[allow(unreachable_code)]
        loop {
            self.commit_logs().await?;

            if self.get_election_state().await == ElectionState::Leader {
                // todo: refactor this block:
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(75)) => {
                        //println!("Sending heartbeat");

                        // todo: move peer connection + sending to separate thread so connection timeout
                        // doesn't result in election timeout
                        let last_log_index = self.get_last_log_index().await;
                        let committed_index = self.get_commit_index().await;
                        let current_term = self.get_current_term().await;
                        for (peer, client) in self.get_peer_clients().await.into_iter() {
                            let (peer_last_log_index, peer_last_log_term, entries) = self.get_logs(&peer).await;
                            let num_entries = entries.len();
                            let mut request = Request::new(EntryRequest {
                                term: current_term,
                                leader_id: self.id.to_string(),
                                prev_log_index: peer_last_log_index,
                                prev_log_term: peer_last_log_term,
                                entries: entries,
                                leader_commit: committed_index
                            });
                            // Todo: see if there's a more idiomatic way to set timeout
                            request.set_timeout(Duration::from_millis(300));

                            // todo: if response term > current_term, switch to follower, same for request_vote
                            match client.lock().await.append_entry(request).await {
                                Ok(response) => {
                                    match response.into_inner().success {
                                        true => {
                                            if num_entries > 0 {
                                                if let Some(value) = self.state.lock().await.next_index.get_mut(&peer) {
                                                    *value = last_log_index + 1;
                                                }

                                                if let Some(value) = self.state.lock().await.match_index.get_mut(&peer) {
                                                    *value = last_log_index;
                                                }
                                            }
                                        },
                                        false => {
                                            // Decrement next_index for peer
                                            if let Some(value) = self.state.lock().await.next_index.get_mut(&peer) {
                                                *value -= 1;
                                            }
                                        }
                                    }
                                },
                                Err(_) => { self.drop_peer_client(&peer).await; }
                            }
                        }

                        // Check if any logs should be commited
                        // Iterating backwards from last log index to (commited_index + 1)
                        let num_peers = self.get_cluster().await?.len();
                        for i in ((committed_index + 1) ..=last_log_index).rev() {
                            // The number of peers where at least log index i has been applied (+1 for leader)
                            let num_peers_applied = self.state.lock().await.match_index.iter().filter(|(_, &applied_index)| applied_index >= i).collect::<Vec<_>>().len() + 1;
                            // Make sure that i is in this term
                            let correct_term = self.state.lock().await.log.iter().filter(|log| log.index == i && log.term != current_term).collect::<Vec<_>>().is_empty();
                            if (num_peers_applied > num_peers / 2) && correct_term {
                                self.state.lock().await.committed_index = i;
                                break
                            }
                        }
                    }
                }
            } else {
                tokio::select! {
                    Some(1) = rx_entries.recv() => {
                        //println!("recv'd heartbeat");
                        continue;
                    }

                    _ = tokio::time::sleep(Duration::from_millis(rng.gen_range(150..300))) => {
                        println!("Timeout: starting election");
                        self.run_election().await?;
                    }
                };
            }
        }

        Ok(())
    }

    async fn run_election(&self) -> Result<(), anyhow::Error> {
        self.set_election_state(ElectionState::Candidate).await;
        self.increment_term().await;
        self.vote_for(Some(self.id)).await;
        println!("Starting election, term: {}", self.get_current_term().await);
        println!("{:?}", self.get_election_state().await);

        let mut num_votes: u32 = 1; // including self

        for (peer, client) in self.get_peer_clients().await.into_iter() {
            println!("sending to client: {:?}", peer);

            let mut request = Request::new(VoteRequest {
                term: self.get_current_term().await,
                candidate_id: self.id.to_string(),
                last_log_index: self.get_last_log_index().await,
                last_log_term: self.get_last_log_term().await,
            });
            request.set_timeout(Duration::from_millis(300));

            let response = match client.lock().await.request_vote(request).await {
                Ok(response) => response.into_inner(),
                Err(_) => {
                    self.drop_peer_client(&peer).await;
                    continue;
                }
            };

            if response.vote_granted {
                println!("Received vote from {:?}", &peer);
                num_votes += 1;
            }
        }

        if num_votes > self.get_cluster().await?.len() as u32 / 2 {
            println!("Elected leader!");
            self.set_election_state(ElectionState::Leader).await;
            self.vote_for(None).await;

            let last_log_index = self.get_last_log_index().await;
            let last_log_term = self.get_last_log_term().await;
            let committed_index = self.get_commit_index().await;
            let current_term = self.get_current_term().await;

            {
                let peers = self.get_peers().await;
                let mut lock = self.state.lock().await;
                lock.next_index = peers
                    .iter()
                    .map(|(peer_id, _)| (peer_id.clone(), last_log_index + 1))
                    .collect::<HashMap<Uuid, u32>>();
                lock.match_index = peers
                    .into_iter()
                    .map(|(peer_id, _)| (peer_id, 0))
                    .collect::<HashMap<Uuid, u32>>();
            }

            // Send empty heartbeat
            for (peer, client) in self.get_peer_clients().await.into_iter() {
                let mut request = Request::new(EntryRequest {
                    term: current_term,
                    leader_id: self.id.to_string(),
                    prev_log_index: last_log_index,
                    prev_log_term: last_log_term,
                    entries: vec![],
                    leader_commit: committed_index,
                });
                request.set_timeout(Duration::from_millis(300));

                if let Err(_) = client.lock().await.append_entry(request).await {
                    self.drop_peer_client(&peer).await;
                }
            }
        }

        Ok(())
    }
}

// todo: access control. checking if id matches leaderid, if request comes from within cluster, etc
#[tonic::async_trait]
impl Raft for Skiff {
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteReply>, Status> {
        println!("received request");

        let current_term = self.get_current_term().await;
        let voted_for = self.get_voted_for().await;

        // todo: get last log index, last log term
        let last_log_index = 0;
        let last_log_term = 0;

        let vote_request = request.into_inner();

        let correct_term = (vote_request.term > current_term && voted_for == Some(self.id))
            || (vote_request.term >= current_term
                && (voted_for.is_none()
                    || voted_for == Some(Uuid::from_str(&vote_request.candidate_id).unwrap())));

        if correct_term
            && vote_request.last_log_index >= last_log_index
            && vote_request.last_log_term >= last_log_term
        {
            println!("Voting for {:?}", vote_request.candidate_id);
            self.vote_for(Some(Uuid::from_str(&vote_request.candidate_id).unwrap()))
                .await;
            self.set_election_state(ElectionState::Follower).await;
            self.set_current_term(vote_request.term).await;

            return Ok(Response::new(VoteReply {
                term: vote_request.term,
                vote_granted: true,
            }));
        }

        Ok(Response::new(VoteReply {
            term: current_term,
            vote_granted: false,
        }))
    }

    async fn append_entry(
        &self,
        request: Request<EntryRequest>,
    ) -> Result<Response<EntryReply>, Status> {
        //println!("recv'd append_entry");
        let entry_request = request.into_inner();
        let current_term = self.get_current_term().await;
        if entry_request.term < current_term {
            return Ok(Response::new(EntryReply {
                term: current_term,
                success: false,
            }));
        }

        // Confirmed that we're receiving requests from a verified leader
        self.set_election_state(ElectionState::Follower).await;
        self.vote_for(None).await;
        self.reset_heartbeat_timer().await;

        if entry_request.prev_log_index > 0 {
            // Todo: Probably come up with a more efficient way to do this
            let mut found_matching_log = false;
            for log in &self.state.lock().await.log {
                if log.index == entry_request.prev_log_index
                    && log.term == entry_request.prev_log_term
                {
                    found_matching_log = true;
                    break;
                }
            }

            if !found_matching_log {
                return Ok(Response::new(EntryReply {
                    term: current_term,
                    success: false,
                }));
            }
        }

        // Todo: Again, probably come up with a better way to do this
        for new_log in &entry_request.entries {
            let mut drop_index: Option<u32> = None;
            for current_log in &self.state.lock().await.log {
                // Find any conflicting logs where the index matches but the term is different
                if current_log.index == new_log.index && current_log.term != entry_request.term {
                    drop_index = Some(current_log.index);
                }
            }

            // Drop any logs with index >= drop_index
            if let Some(drop_index) = drop_index {
                self.state
                    .lock()
                    .await
                    .log
                    .retain(|log| log.index < drop_index);
            }
        }

        if entry_request.entries.len() > 0 {
            println!("{:?}", entry_request.entries);
        }

        let last_log_index = self.get_last_log_index().await;

        // Append new entries to the log
        for new_log in entry_request.entries {
            if new_log.index > last_log_index {
                println!("Adding to log");
                self.state.lock().await.log.push(Log {
                    index: new_log.index,
                    term: new_log.term,
                    action: match new_log.action {
                        // Todo: fix this atrocity. I don't want to hard code i32 values
                        x if x == raft_proto::Action::Insert as i32 => {
                            Action::Insert(new_log.key, new_log.value.unwrap())
                        }
                        x if x == raft_proto::Action::Delete as i32 => Action::Delete(new_log.key),

                        // Todo: this could be implemented better
                        // At the moment, we're deserializing it so that get_cluster() can return Vec<Uuid, Ipv4Addr>,
                        // but we're also deserializing it before it gets serialized again when committed.
                        // Not a huge deal, but not very efficient
                        x if x == raft_proto::Action::Configure as i32 => Action::Configure(
                            bincode::deserialize(&new_log.value.unwrap()).unwrap(),
                        ),
                        _ => return Err(Status::invalid_argument("Invalid action")),
                    },
                    committed: Arc::new((Mutex::new(false), Notify::new())),
                });
            }
        }

        if entry_request.leader_commit > self.get_commit_index().await {
            self.state.lock().await.committed_index =
                min(entry_request.leader_commit, self.get_last_log_index().await);
        }

        Ok(Response::new(EntryReply {
            term: current_term,
            success: true,
        }))
    }

    // Todo: this should only be called for the leader
    // We need to handle forwarding to leader
    async fn add_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        println!("Adding server to cluster");
        let new_server = request.into_inner();
        let new_uuid = Uuid::from_str(&new_server.id).unwrap();

        let mut cluster_config: Vec<(String, Ipv4Addr)> = self
            .get_cluster()
            .await
            .unwrap()
            .into_iter()
            .map(|(id, addr)| (id.to_string(), addr))
            .collect();
        cluster_config.push((new_server.id, Ipv4Addr::from_str(&new_server.address).unwrap()));

        self.log(Action::Configure(cluster_config.clone())).await;
        let last_log_index = self.get_last_log_index().await;
        self.state.lock().await.next_index.insert(new_uuid, last_log_index + 1);
        self.state.lock().await.match_index.insert(new_uuid, 0);

        Ok(Response::new(ServerReply {
            success: true,
            cluster: Some(bincode::serialize(&cluster_config).unwrap()),
        }))
    }

    async fn remove_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        todo!()
    }
}

#[tonic::async_trait]
impl skiff_proto::skiff_server::Skiff for Skiff {
    // Todo: add tree functionality
    // Todo: forward insert and delete requests to the leader

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let get_request = request.into_inner();
        let value = self.state.lock().await.conn.get(get_request.key);

        match value {
            Ok(inner1) => match inner1 {
                Some(data) => Ok(Response::new(GetReply {
                    value: Some(data.to_vec()),
                })),
                None => Ok(Response::new(GetReply { value: None })),
            },
            Err(_) => Err(Status::internal("failed to query sled db")),
        }
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertReply>, Status> {
        let insert_request = request.into_inner();
        let commit_arc = self
            .log(Action::Insert(insert_request.key, insert_request.value))
            .await;

        // Wait until the log is committed
        // Todo: implement timeout
        let (_, notify) = &*commit_arc;
        notify.notified().await;
        Ok(Response::new(InsertReply { success: true }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteReply>, Status> {
        let delete_request = request.into_inner();
        let commit_arc = self.log(Action::Delete(delete_request.key)).await;

        // Wait until the log is committed
        // Todo: implement timeout
        let (_, notify) = &*commit_arc;
        notify.notified().await;
        Ok(Response::new(DeleteReply { success: true }))
    }
}
