pub mod skiff_proto {
    tonic::include_proto!("skiff");
}

use crate::error::Error;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use skiff_proto::{
    skiff_client::SkiffClient, skiff_server::SkiffServer, DeleteReply, DeleteRequest, GetReply,
    GetRequest, InsertReply, InsertRequest,
};
use skiff_proto::{
    Empty, EntryReply, EntryRequest, ListKeysReply, ListKeysRequest, PrefixReply, ServerReply,
    ServerRequest, SubscribeReply, SubscribeRequest, VoteReply, VoteRequest,
};
use std::cmp::min;
use std::pin::Pin;
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};

use tokio::sync::{mpsc, Mutex, Notify};
use tonic::{transport::Channel, Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Action {
    Insert(String, Vec<u8>),
    Delete(String),
    Configure(HashMap<Uuid, Ipv4Addr>),
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
    Follower(Uuid),
}

#[derive(Debug, Clone)]
struct State {
    election_state: ElectionState,
    current_term: u32,
    voted_for: Option<Uuid>,
    committed_index: u32,
    last_applied: u32,

    peer_clients: HashMap<Uuid, Arc<Mutex<SkiffClient<Channel>>>>,

    next_index: HashMap<Uuid, u32>,
    match_index: HashMap<Uuid, u32>,

    log: Vec<Log>,
    conn: sled::Db,
}

#[derive(Debug, Clone)]
pub struct Skiff {
    id: Uuid,
    address: Ipv4Addr,
    state: Arc<Mutex<State>>,
    tx_entries: Arc<Mutex<mpsc::Sender<u8>>>,
    rx_entries: Arc<Mutex<mpsc::Receiver<u8>>>,
    subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<SubscribeReply>>>>>,
}

// Todo: env logging
// Todo: add clap, cli params
// Todo: add configuration parameters for
// - when to drop unresponsive peer
// - threshold for automatically snapshotting
// and add corresponding functionality
// Todo: allow custom port, swap out Ipv4Addr w SocketAddr where appropriate
// Todo: separate address into bind_addr and advertise_addr
// Todo: docs
// Todo: publish to crates.io
impl Skiff {
    pub fn new(
        id: Uuid,
        address: Ipv4Addr,
        data_dir: String,
        peers: Vec<Ipv4Addr>,
    ) -> Result<Self, Error> {
        let conn = sled::open(data_dir)?;
        let (tx_entries, rx_entries) = mpsc::channel(32);

        // First convert peers into a vector of tuples (Uuid, Ipv4Addr)
        // We don't know the actual uuids, we will retrieve them once the server starts
        // For now, intialize with nil Uuids
        // Finally add this node to the cluster
        let mut cluster: HashMap<Uuid, Ipv4Addr> = peers
            .into_iter()
            .map(|addr| (Uuid::new_v4(), addr))
            .collect();

        cluster.insert(id, address);

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
                election_state: ElectionState::Follower(Uuid::nil()),
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
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // todo: initializing a new cluster with a known config without needing to send add_server rpc

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn get_address(&self) -> Ipv4Addr {
        self.address
    }

    // This was named is_ready, but is_leader_elected is more accurate
    // Consider renaming to is_ready as that's what we're trying to evaluate.
    //  should consider logic further
    pub async fn is_leader_elected(&self) -> bool {
        let election_state = self.get_election_state().await;
        match election_state {
            ElectionState::Leader => true,
            ElectionState::Candidate => false,
            ElectionState::Follower(id) => {
                if Uuid::nil() == id {
                    return false;
                }

                true
            }
        }
    }

    // Todo: consider if this should return result, although there should always be a cluster_config log
    // handle missing cluster configuration? sole node in cluster?
    pub async fn get_cluster(&self) -> Result<HashMap<Uuid, Ipv4Addr>, Error> {
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
                _ => return Err(Error::MissingClusterConfig),
            },
            _ => return Err(Error::MissingClusterConfig),
        };

        Ok(config)
    }

    async fn get_peers(&self) -> HashMap<Uuid, Ipv4Addr> {
        self.get_cluster()
            .await
            // Todo: figure out if unwrap is sufficient or if this needs to return Result
            // affects get_peer_client()
            .unwrap()
            .into_iter()
            .filter(|(_, addr)| *addr != self.address)
            .collect()
    }

    async fn get_peer_client(
        &self,
        peer: &Uuid,
    ) -> Result<Arc<Mutex<SkiffClient<Channel>>>, Error> {
        let peers = self.get_peers().await;

        if !peers.contains_key(peer) {
            return Err(Error::PeerNotFound);
        }

        if let Some(client) = self.state.lock().await.peer_clients.get(peer) {
            return Ok(client.clone());
        }

        match SkiffClient::connect(format!(
            "http://{}",
            SocketAddrV4::new(*peers.get(peer).unwrap(), 9400)
        ))
        .await
        {
            Ok(client) => {
                let arc = Arc::new(Mutex::new(client));
                self.state
                    .lock()
                    .await
                    .peer_clients
                    .insert(peer.to_owned(), arc.clone());
                Ok(arc)
            }
            Err(_) => Err(Error::PeerConnectFailed),
        }
    }

    async fn drop_peer_client(&self, id: &Uuid) {
        let mut lock = self.state.lock().await;
        let _ = lock.peer_clients.remove(id);
    }

    // todo: consider making these macros
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

    async fn get_prefixes(&self) -> Result<Vec<String>, Error> {
        match self.state.lock().await.conn.get("trees")? {
            Some(tree_vec) => match bincode::deserialize::<Vec<String>>(&tree_vec) {
                Ok(trees) => Ok(trees),
                Err(_) => Err(Error::DeserializeFailed),
            },
            None => Ok(vec![]),
        }
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let mut keys = vec![];
        let trees = self.get_prefixes().await?;

        // Todo: de-duplicate code
        if prefix.is_empty() || prefix == "/" {
            let tree = self.state.lock().await.conn.open_tree("base")?;

            tree.into_iter()
                .keys()
                .map(|key| String::from_utf8(key.unwrap().to_vec()).unwrap())
                .for_each(|key| {
                    keys.push(key);
                });
        }

        for tree_name in &trees {
            let prefix_trimmed = match prefix.ends_with("/") {
                true => prefix.trim_end_matches("/"),
                false => prefix,
            };

            if tree_name.starts_with(prefix_trimmed) {
                let tree = self
                    .state
                    .lock()
                    .await
                    .conn
                    .open_tree(format!("base_{}", tree_name.replace("/", "_")))?;

                tree.into_iter()
                    .keys()
                    .map(|key| String::from_utf8(key.unwrap().to_vec()).unwrap())
                    .for_each(|key| {
                        keys.push(format!("{}/{}", tree_name, key));
                    });
            }
        }

        Ok(keys)
    }

    async fn get_logs(&self, peer: &Uuid) -> (u32, u32, Vec<skiff_proto::Log>) {
        let lock = self.state.lock().await;
        let log_next_index: &u32 = lock.next_index.get(peer).unwrap();
        let mut prev_log_index = 0;
        let mut prev_log_term = 0;
        let mut new_logs: Vec<skiff_proto::Log> = vec![];

        for log in &lock.log {
            // Get latest log that should already be in peer's log
            if log.index < *log_next_index && log.index > prev_log_index {
                prev_log_index = log.index;
                prev_log_term = log.term;
            } else if log.index >= *log_next_index {
                new_logs.push(match &log.action {
                    Action::Insert(key, value) => skiff_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: skiff_proto::Action::Insert as i32,
                        key: key.clone(),
                        value: Some(value.clone()),
                    },
                    Action::Delete(key) => skiff_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: skiff_proto::Action::Delete as i32,
                        key: key.clone(),
                        value: None,
                    },
                    Action::Configure(config) => skiff_proto::Log {
                        index: log.index,
                        term: log.term,
                        action: skiff_proto::Action::Configure as i32,
                        key: "cluster".to_string(),
                        value: Some(bincode::serialize(&config).unwrap()),
                    },
                });
            }
        }

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
                // Todo: handle and document difference between "key" and "/key"
                Action::Insert(key, value) => {
                    let full_key = key.clone();
                    let mut tree_parts: Vec<&str> = key.split("/").collect();
                    let key = tree_parts.pop().unwrap();

                    let prefix = tree_parts.join("/");
                    let tree_name = match prefix.len() {
                        0 => "base".to_string(),
                        _ => {
                            self.state
                                .lock()
                                .await
                                .conn
                                .update_and_fetch("trees", |trees| match trees {
                                    Some(tree_vec) => {
                                        let mut updated_tree_vec =
                                            bincode::deserialize::<Vec<String>>(tree_vec).unwrap();

                                        if !updated_tree_vec.contains(&prefix) {
                                            updated_tree_vec.push(prefix.clone());
                                        }

                                        Some(bincode::serialize(&updated_tree_vec).unwrap())
                                    }
                                    None => Some(bincode::serialize(&vec![&prefix]).unwrap()),
                                });

                            format!("base_{}", &prefix.replace("/", "_"))
                        }
                    };

                    // Todo: maybe alert subscribers on delete
                    let mut subscribers = self.subscribers.lock().await;
                    for (sub_prefix, senders) in subscribers.iter_mut() {
                        // Todo: this could cause issues when the provided prefix is parent/ and tree is ex. parents/ or parent1/, etc
                        if prefix.starts_with(sub_prefix.trim_end_matches("/")) {
                            let mut drop_senders = Vec::new();
                            for (i, sender) in senders.iter_mut().enumerate() {
                                // Todo: delete the sender if this receives an error
                                match sender
                                    .send(SubscribeReply {
                                        key: full_key.clone(),
                                        action: skiff_proto::Action::Insert as i32,
                                        value: Some(value.clone()),
                                    })
                                    .await
                                {
                                    Ok(_) => continue,
                                    Err(_) => {
                                        drop_senders.push(i);
                                    }
                                }
                            }
                            for i in drop_senders {
                                senders.remove(i);
                            }
                        }
                    }

                    let tree = self.state.lock().await.conn.open_tree(tree_name)?;
                    tree.insert(key, value)?;
                }
                Action::Delete(key) => {
                    let mut tree_parts: Vec<&str> = key.split("/").collect();
                    let key = tree_parts.pop().unwrap();

                    let prefix = tree_parts.join("/");
                    let tree_name = match prefix.len() {
                        0 => "base".to_string(),
                        _ => format!("base_{}", &prefix.replace("/", "_")),
                    };

                    let trees = self.state.lock().await.conn.tree_names();
                    if trees.contains(&tree_name.as_bytes().into()) {
                        let lock = self.state.lock().await;
                        let tree = lock.conn.open_tree(&tree_name)?;
                        tree.remove(key)?;

                        if tree.is_empty() {
                            let _ = lock.conn.drop_tree(&tree_name);
                            lock.conn.update_and_fetch("trees", |trees| match trees {
                                Some(tree_vec) => {
                                    let mut updated_tree_vec =
                                        bincode::deserialize::<Vec<String>>(tree_vec).unwrap();

                                    println!("{:?}, {}", updated_tree_vec, tree_name);
                                    if let Some(index) =
                                        updated_tree_vec.iter().position(|name| name == &prefix)
                                    {
                                        updated_tree_vec.remove(index);
                                    }

                                    Some(bincode::serialize(&updated_tree_vec).unwrap())
                                }
                                None => Some(bincode::serialize(&Vec::<String>::new()).unwrap()),
                            });
                        }
                    }
                }
                Action::Configure(config) => {
                    // Todo: also save committed_index and last_applied for resuming operation later
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

    // Todo: consider making this async, waiting for cluster join to finish
    pub fn initialize_service(&self) -> SkiffServer<Skiff> {
        let skiff = self.clone();
        let _: tokio::task::JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
            // Todo: when we're restoring a cluster from previous operation (ex. after outage / migration)
            // the cluster len for all nodes will be > 1, but no leader exists yet, so add_server rpc fails.
            // Might be ok if leader election happens subsequently, but this should a) be verified
            // and b) logic for this scenario should be made obvious
            if skiff.get_cluster().await.unwrap().len() > 1 {
                println!("Joining cluster");

                let mut joined_cluster = false;
                for id in skiff.get_peers().await.keys() {
                    println!("Asking {:?} to join cluster", id);

                    let mut request = Request::new(ServerRequest {
                        id: skiff.id.to_string(),
                        address: skiff.address.to_string(),
                    });

                    request.set_timeout(Duration::from_millis(300));

                    let client_arc = skiff.get_peer_client(id).await.unwrap();
                    let mut client = client_arc.lock().await;
                    match client.add_server(request).await {
                        Ok(response) => {
                            let inner = response.into_inner();
                            if inner.success {
                                if let Some(cluster) = inner.cluster {
                                    // Todo: this will get logged twice. Once here and once from append_entry
                                    // It shouldn't be an issue, but this is an unecessary duplication
                                    skiff
                                        .log(Action::Configure(
                                            bincode::deserialize(&cluster).unwrap(),
                                        ))
                                        .await;

                                    joined_cluster = true;

                                    break;
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }

                if !joined_cluster {
                    return Err(Error::ClusterJoinFailed.into());
                }
            }
            // Todo: if we are lone node in cluster we can skip election timeout and make ourselves leader

            let mut server1 = skiff.clone();
            let _elections: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
                tokio::spawn(async move {
                    server1.election_manager().await?;
                    Ok(())
                });

            Ok(())
        });

        SkiffServer::new(self.clone())
    }

    // Todo: Add token for authenticated joins
    // Todo: snapshot / log compaction, resuming operation after restart
    pub async fn start(self) -> Result<(), anyhow::Error> {
        let service = self.initialize_service();

        let bind_address = SocketAddr::new(self.address.into(), 9400);
        tonic::transport::Server::builder()
            .add_service(service)
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
                        for peer in self.get_peers().await.keys() {
                            let (peer_last_log_index, peer_last_log_term, entries) = self.get_logs(peer).await;
                            let num_entries = entries.len();
                            let mut request = Request::new(EntryRequest {
                                term: current_term,
                                leader_id: self.id.to_string(),
                                prev_log_index: peer_last_log_index,
                                prev_log_term: peer_last_log_term,
                                entries,
                                leader_commit: committed_index
                            });
                            // Todo: see if there's a more idiomatic way to set timeout
                            request.set_timeout(Duration::from_millis(300));

                            // todo: if response term > current_term, switch to follower, same for request_vote
                            let client_arc = self.get_peer_client(peer).await.unwrap();
                            let mut client = client_arc.lock().await;
                            match client.append_entry(request).await {
                                Ok(response) => {
                                    match response.into_inner().success {
                                        true => {
                                            if num_entries > 0 {
                                                if let Some(value) = self.state.lock().await.next_index.get_mut(peer) {
                                                    *value = last_log_index + 1;
                                                }

                                                if let Some(value) = self.state.lock().await.match_index.get_mut(peer) {
                                                    *value = last_log_index;
                                                }
                                            }
                                        },
                                        false => {
                                            // Decrement next_index for peer
                                            if let Some(value) = self.state.lock().await.next_index.get_mut(peer) {
                                                *value -= 1;
                                            }
                                        }
                                    }
                                },
                                Err(_) => { self.drop_peer_client(peer).await; }
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

        for peer in self.get_peers().await.keys() {
            println!("sending to client: {:?}", peer);

            let mut request = Request::new(VoteRequest {
                term: self.get_current_term().await,
                candidate_id: self.id.to_string(),
                last_log_index: self.get_last_log_index().await,
                last_log_term: self.get_last_log_term().await,
            });
            request.set_timeout(Duration::from_millis(300));

            let client = self.get_peer_client(peer).await.unwrap();
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
                    .keys()
                    .map(|peer_id| (*peer_id, last_log_index + 1))
                    .collect::<HashMap<Uuid, u32>>();
                lock.match_index = peers
                    .into_keys()
                    .map(|peer_id| (peer_id, 0))
                    .collect::<HashMap<Uuid, u32>>();
            }

            // Send empty heartbeat
            for id in self.get_peers().await.keys() {
                let mut request = Request::new(EntryRequest {
                    term: current_term,
                    leader_id: self.id.to_string(),
                    prev_log_index: last_log_index,
                    prev_log_term: last_log_term,
                    entries: vec![],
                    leader_commit: committed_index,
                });
                request.set_timeout(Duration::from_millis(300));

                let client_arc = self.get_peer_client(id).await.unwrap();
                let mut client = client_arc.lock().await;
                if let Err(_) = client.append_entry(request).await {
                    self.drop_peer_client(id).await;
                }
            }
        }

        Ok(())
    }
}

// todo: access control. checking if id matches leaderid, if request comes from within cluster, etc
#[tonic::async_trait]
impl skiff_proto::skiff_server::Skiff for Skiff {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SubscribeReply, Status>> + Send>>;

    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteReply>, Status> {
        println!("received vote request");

        let current_term = self.get_current_term().await;
        let voted_for = self.get_voted_for().await;
        let last_log_index = self.get_last_log_index().await;
        let last_log_term = self.get_last_log_term().await;

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
            self.set_election_state(ElectionState::Follower(
                Uuid::from_str(&vote_request.candidate_id).unwrap(),
            ))
            .await;
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
        self.set_election_state(ElectionState::Follower(
            Uuid::from_str(&entry_request.leader_id).unwrap(),
        ))
        .await;
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

        // if entry_request.entries.len() > 0 {
        //     println!("{:?}", entry_request.entries);
        // }

        let last_log_index = self.get_last_log_index().await;

        // Append new entries to the log
        for new_log in entry_request.entries {
            if new_log.index > last_log_index {
                println!("Adding to log");
                self.state.lock().await.log.push(Log {
                    index: new_log.index,
                    term: new_log.term,
                    action: match skiff_proto::Action::try_from(new_log.action) {
                        Ok(skiff_proto::Action::Insert) => {
                            Action::Insert(new_log.key, new_log.value.unwrap())
                        }
                        Ok(skiff_proto::Action::Delete) => Action::Delete(new_log.key),
                        Ok(skiff_proto::Action::Configure) => Action::Configure(
                            bincode::deserialize(&new_log.value.unwrap()).unwrap(),
                        ),
                        Err(_) => return Err(Status::invalid_argument("Invalid action")),
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

    async fn add_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.add_server(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

        println!("Adding server to cluster");
        let new_server = request.into_inner();
        let new_uuid = Uuid::from_str(&new_server.id).unwrap();

        let mut cluster_config: HashMap<Uuid, Ipv4Addr> = self.get_cluster().await.unwrap();

        let id = Uuid::from_str(&new_server.id).unwrap();
        let addr = Ipv4Addr::from_str(&new_server.address).unwrap();

        if cluster_config.get(&id).is_none() {
            cluster_config.insert(id, addr);
            self.log(Action::Configure(cluster_config.clone())).await;
        }

        let last_log_index = self.get_last_log_index().await;
        self.state
            .lock()
            .await
            .next_index
            .insert(new_uuid, last_log_index + 1);
        self.state.lock().await.match_index.insert(new_uuid, 0);

        Ok(Response::new(ServerReply {
            success: true,
            cluster: Some(bincode::serialize(&cluster_config).unwrap()),
        }))
    }

    // Todo: no part of the program calls this yet
    async fn remove_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        todo!()
    }

    // Todo: maybe add watch_prefix function that communicates changes to clients

    // Todo: Forwarding to the leader fails when... there is no leader, or when we are a candiate
    // This is an issue when calling add_server from a follower to a server before the latter has elected itself

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        // If follower, connect to leader and forward request
        // Todo: ideally get requests could be done locally w/o forwarding, which would improve performance
        // However, if the client makes an insert or delete request then immediately makes a get request,
        // the change could have been logged locally and committed by the leader without the change
        // being made to the state machine (sled) locally (until the subsequent append_entries call),
        // resulting in an outdated get. The current workaround is to just forward the request.
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.get(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

        let get_request = request.into_inner();
        let mut tree_parts: Vec<&str> = get_request.key.split("/").collect();
        let key = tree_parts.pop().unwrap();

        let mut tree_name = tree_parts.join("/");
        tree_name = match tree_name.len() {
            0 => "base".to_string(),
            _ => format!("base_{}", tree_name.replace("/", "_")),
        };

        if let Ok(tree) = self.state.lock().await.conn.open_tree(tree_name) {
            let value = tree.get(key);
            match value {
                Ok(inner1) => match inner1 {
                    Some(data) => Ok(Response::new(GetReply {
                        value: Some(data.to_vec()),
                    })),
                    None => Ok(Response::new(GetReply { value: None })),
                },
                Err(_) => Err(Status::internal("failed to query sled db")),
            }
        } else {
            Err(Status::internal("failed to open sled tree"))
        }
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertReply>, Status> {
        // If follower, connect to leader and forward request
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.insert(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

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
        // If follower, connect to leader and forward request
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.delete(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

        let delete_request = request.into_inner();
        let commit_arc = self.log(Action::Delete(delete_request.key)).await;

        // Wait until the log is committed
        // Todo: implement timeout
        let (_, notify) = &*commit_arc;
        notify.notified().await;
        Ok(Response::new(DeleteReply { success: true }))
    }

    async fn get_prefixes(&self, request: Request<Empty>) -> Result<Response<PrefixReply>, Status> {
        // If follower, connect to leader and forward request
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.get_prefixes(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

        match self.get_prefixes().await {
            Ok(prefixes) => Ok(Response::new(PrefixReply { prefixes })),
            Err(_) => Err(Status::internal("failed to get prefixes")),
        }
    }

    async fn list_keys(
        &self,
        request: Request<ListKeysRequest>,
    ) -> Result<Response<ListKeysReply>, Status> {
        // If follower, connect to leader and forward request
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.list_keys(request).await;
            }

            return Err(Status::internal("failed to forward request to leader"));
        }

        match self.list_keys(request.into_inner().prefix.as_str()).await {
            Ok(keys) => Ok(Response::new(ListKeysReply { keys })),
            Err(_) => Err(Status::internal("failed to get keys")),
        }
    }

    // This shouldn't need forwarding to leader
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let prefix = request.into_inner().prefix;

        let (sender, receiver) = mpsc::channel(32);
        let mut subscribers = self.subscribers.lock().await;
        if !subscribers.contains_key(&prefix) {
            subscribers.insert(prefix.clone(), Vec::new());
        }

        let senders = subscribers.get_mut(&prefix).unwrap();
        senders.push(sender);

        let stream = ReceiverStream::new(receiver).map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
