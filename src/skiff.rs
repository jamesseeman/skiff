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
use std::{collections::HashMap, thread::current};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Channel, Request, Response, Status};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Action {
    Insert(String, Vec<u8>),
    Delete(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log {
    index: u32,
    term: u32,
    action: Action,
    committed: bool,
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
    voted_for: Option<u32>,
    committed_index: u32,
    last_applied: u32,

    // todo: potentically separate cluster logic
    cluster: Vec<Ipv4Addr>,
    peer_clients: HashMap<Ipv4Addr, Arc<Mutex<RaftClient<Channel>>>>,

    // todo: potentially separate leader state
    next_index: HashMap<Ipv4Addr, u32>,
    match_index: HashMap<Ipv4Addr, u32>,

    log: Vec<Log>,
    conn: sled::Tree,
}

#[derive(Debug, Clone)]
pub struct Skiff {
    id: u32,
    address: Ipv4Addr,
    state: Arc<Mutex<State>>,
    tx_entries: Arc<Mutex<mpsc::Sender<u8>>>,
    rx_entries: Arc<Mutex<mpsc::Receiver<u8>>>,
}

impl Skiff {
    pub fn new(address: Ipv4Addr) -> Result<Self, SkiffError> {
        if !Path::new("/tmp/skiff/").exists() {
            fs::create_dir("/tmp/skiff/")?;
        }

        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
        let id: u32 = rng.gen();
        let conn = sled::open("/tmp/skiff/")?.open_tree(id.to_string())?;
        let (tx_entries, rx_entries) = mpsc::channel(32);

        Ok(Skiff {
            id,
            address,
            state: Arc::new(Mutex::new(State {
                election_state: ElectionState::Follower,
                current_term: 0,
                voted_for: None,
                committed_index: 0,
                last_applied: 0,
                cluster: vec![],
                peer_clients: HashMap::new(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                log: vec![],
                conn,
            })),
            tx_entries: Arc::new(Mutex::new(tx_entries)),
            rx_entries: Arc::new(Mutex::new(rx_entries)),
        })
    }

    // TODO: cluster: Vec<(Uuid, Ipv4Addr)> or something similar... this for a previously existing cluster
    pub fn from_cluster(
        address: Ipv4Addr,
        cluster: Vec<Ipv4Addr>,
        data_dir: &str,
    ) -> Result<Skiff, SkiffError> {
        cluster
            .iter()
            .find(|node| **node == address)
            .ok_or(SkiffError::InvalidAddress)?;

        if !Path::new(data_dir).exists() {
            fs::create_dir(data_dir)?;
        }

        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
        let id: u32 = rng.gen();
        let conn = sled::open(data_dir)?.open_tree(id.to_string())?;
        let (tx_entries, rx_entries) = mpsc::channel(32);

        Ok(Skiff {
            id,
            address,
            state: Arc::new(Mutex::new(State {
                election_state: ElectionState::Follower,
                current_term: 0,
                voted_for: None,
                committed_index: 0,
                last_applied: 0,
                cluster: cluster,
                peer_clients: HashMap::new(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                log: vec![],
                conn,
            })),
            tx_entries: Arc::new(Mutex::new(tx_entries)),
            rx_entries: Arc::new(Mutex::new(rx_entries)),
        })
    }

    // Todo: consider making these macros
    pub fn get_address(&self) -> Ipv4Addr {
        self.address
    }

    async fn get_cluster(&self) -> Vec<Ipv4Addr> {
        self.state.lock().await.cluster.clone()
    }

    async fn get_peers(&self) -> Vec<Ipv4Addr> {
        self.state
            .lock()
            .await
            .cluster
            .clone()
            .into_iter()
            .filter(|addr| *addr != self.address)
            .collect()
    }

    async fn get_peer_clients(&self) -> Vec<(Ipv4Addr, Arc<Mutex<RaftClient<Channel>>>)> {
        let peers = self.get_peers().await;

        let lock = self.state.lock().await;
        let mut retry = vec![];
        let mut peer_clients: Vec<(Ipv4Addr, Arc<Mutex<RaftClient<Channel>>>)> = peers
            .into_iter()
            .filter_map(|addr| match lock.peer_clients.get(&addr) {
                Some(client) => Some((addr, client.clone())),
                None => {
                    retry.push(addr);
                    None
                }
            })
            .collect();

        // todo: bit sloppy, try to fix
        drop(lock);
        let mut lock = self.state.lock().await;

        for peer in retry {
            match RaftClient::connect(format!("http://{}", SocketAddrV4::new(peer, 9400))).await {
                Ok(client) => {
                    let arc = Arc::new(Mutex::new(client));
                    lock.peer_clients.insert(peer, arc.clone());
                    peer_clients.push((peer, arc));
                }
                Err(_) => {
                    println!("Failed to connect");
                }
            }
        }

        peer_clients
    }

    async fn drop_peer_client(&self, addr: Ipv4Addr) {
        let mut lock = self.state.lock().await;
        let _ = lock.peer_clients.remove(&addr);
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

    async fn vote_for(&self, candidate_id: Option<u32>) {
        self.state.lock().await.voted_for = candidate_id;
    }

    async fn get_voted_for(&self) -> Option<u32> {
        self.state.lock().await.voted_for
    }

    async fn log(&self, action: Action) {
        let mut lock = self.state.lock().await;
        let current_term = lock.current_term;
        let last_index = lock.log.last().map(|last: &Log| last.index).unwrap_or(0);
        lock.log.push(Log {
            index: last_index + 1,
            term: current_term,
            action,
            committed: false,
        });

        println!("{:?}", lock.log);
    }

    async fn get_logs(&self, peer: &Ipv4Addr) -> (u32, u32, Vec<raft_proto::Log>) {
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
                        action: raft_proto::Action::Insert as i32,
                        key: key.clone(),
                        value: Some(value.clone()),
                    },
                    Action::Delete(key) => raft_proto::Log {
                        index: log.index,
                        action: raft_proto::Action::Delete as i32,
                        key: key.clone(),
                        value: None,
                    },
                });
            }
        }

        (prev_log_index, prev_log_term, new_logs)
    }

    async fn commit_logs(&self, new_commit_index: u32) -> Result<(), sled::Error> {
        println!("Committing");

        let new_logs: Vec<(usize, Action)> = self
            .state
            .lock()
            .await
            .log
            .iter()
            .enumerate()
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
            }

            if let Some(log) = self.state.lock().await.log.get_mut(i) {
                log.committed = true;
            }
        }

        self.state.lock().await.committed_index = new_commit_index;

        Ok(())
    }

    async fn reset_heartbeat_timer(&self) {
        let _ = self.tx_entries.lock().await.send(1).await; // Can be anything
    }

    // TODO: Add token for authenticated joins
    pub fn join_cluster(&self, address: Ipv4Addr) -> Result<(), SkiffError> {
        Err(SkiffError::ClusterJoinFailed)
    }

    pub fn init_cluster(&self) -> Result<(), SkiffError> {
        Ok(())
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
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

    async fn election_manager(&mut self) -> Result<(), anyhow::Error> {
        let mut rng = {
            let rng = rand::thread_rng();
            StdRng::from_rng(rng)?
        };

        let mut rx_entries = self.rx_entries.lock().await;

        #[allow(unreachable_code)]
        loop {
            if self.get_election_state().await == ElectionState::Leader {
                // todo: refactor this block:
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(75)) => {
                        //println!("Sending heartbeat");

                        // todo: move peer connection + sending to separate thread so connection timeout
                        // doesn't result in election timeout

                        let committed_index = self.get_commit_index().await;
                        let current_term = self.get_current_term().await;
                        for (peer, client) in self.get_peer_clients().await.into_iter() {
                            let (last_log_index, last_log_term, entries) = self.get_logs(&peer).await;
                            let num_entries = entries.len();
                            let mut request = Request::new(EntryRequest {
                                term: current_term,
                                leader_id: self.id,
                                prev_log_index: last_log_index,
                                prev_log_term: last_log_term,
                                entries: entries,
                                leader_commit: committed_index
                            });
                            request.set_timeout(Duration::from_millis(300));

                            match client.lock().await.append_entry(request).await {
                                Ok(response) => {
                                    // todo: handle success (committing) and failures (decrement next_index)

                                    //println!("{}", response.into_inner().success);
                                    //if num_entries > 0 {
                                    //    println!("{:?}", response)
                                    //}
                                    continue
                                },
                                Err(_) => { self.drop_peer_client(peer).await; }
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
        println!("Starting election, term: {}", self.get_current_term().await);
        println!("{:?}", self.get_election_state().await);

        self.increment_term().await;
        self.vote_for(Some(self.id)).await;

        let mut num_votes: u32 = 1; // including self

        for (peer, client) in self.get_peer_clients().await.into_iter() {
            println!("sending to client: {:?}", peer);

            let mut request = Request::new(VoteRequest {
                term: self.get_current_term().await,
                candidate_id: self.id,
                last_log_index: self.get_last_log_index().await,
                last_log_term: self.get_last_log_term().await,
            });
            request.set_timeout(Duration::from_millis(300));

            let response = match client.lock().await.request_vote(request).await {
                Ok(response) => response.into_inner(),
                Err(_) => {
                    self.drop_peer_client(peer).await;
                    continue;
                }
            };

            if response.vote_granted {
                println!("Received vote from {:?}", &peer);
                num_votes += 1;
            }
        }

        if num_votes > self.get_cluster().await.len() as u32 / 2 {
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
                    .map(|peer| (peer.clone(), last_log_index + 1))
                    .collect::<HashMap<Ipv4Addr, u32>>();
                lock.match_index = peers
                    .into_iter()
                    .map(|peer| (peer, 0))
                    .collect::<HashMap<Ipv4Addr, u32>>();
            }

            // Send empty heartbeat
            for (peer, client) in self.get_peer_clients().await.into_iter() {
                let mut request = Request::new(EntryRequest {
                    term: current_term,
                    leader_id: self.id,
                    prev_log_index: last_log_index,
                    prev_log_term: last_log_term,
                    entries: vec![],
                    leader_commit: committed_index,
                });
                request.set_timeout(Duration::from_millis(300));

                if let Err(_) = client.lock().await.append_entry(request).await {
                    self.drop_peer_client(peer).await;
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
                && (voted_for.is_none() || voted_for == Some(vote_request.candidate_id)));

        if correct_term
            && vote_request.last_log_index >= last_log_index
            && vote_request.last_log_term >= last_log_term
        {
            println!("Voting for {:?}", vote_request.candidate_id);
            self.vote_for(Some(vote_request.candidate_id)).await;
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
                    term: entry_request.term,
                    action: match new_log.action {
                        // Todo: fix this atrocity. I don't want to hard code i32 values
                        x if x == raft_proto::Action::Insert as i32 => {
                            Action::Insert(new_log.key, new_log.value.unwrap())
                        }
                        x if x == raft_proto::Action::Delete as i32 => Action::Delete(new_log.key),
                        _ => return Err(Status::invalid_argument("Invalid action")),
                    },
                    committed: false,
                });
            }
        }

        if entry_request.leader_commit > self.get_commit_index().await {
            println!("Commiting logs");
            let new_commit_index =
                min(entry_request.leader_commit, self.get_last_log_index().await);
            self.commit_logs(new_commit_index).await;
        }

        Ok(Response::new(EntryReply {
            term: current_term,
            success: true,
        }))
    }

    async fn add_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        todo!()
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
        self.log(Action::Insert(insert_request.key, insert_request.value))
            .await;

        Ok(Response::new(InsertReply { success: true }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteReply>, Status> {
        let delete_request = request.into_inner();
        self.log(Action::Delete(delete_request.key)).await;

        Ok(Response::new(DeleteReply { success: true }))
    }
}
