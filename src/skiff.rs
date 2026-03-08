pub(crate) mod skiff_proto {
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

use tokio::sync::{mpsc, watch, Mutex, Notify};
use tonic::{transport::Channel, Request, Response, Status};
use tracing::{debug, info, trace};
use uuid::Uuid;

const RAFT_META_TREE: &str = "__raft_meta";
const RAFT_LOG_TREE: &str = "__raft_log";
const KEY_CURRENT_TERM: &[u8] = b"current_term";
const KEY_VOTED_FOR: &[u8] = b"voted_for";
const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_NODE_ID: &[u8] = b"node_id";

#[derive(Serialize, Deserialize)]
struct PersistedLog {
    index: u32,
    term: u32,
    action: Action,
}

fn persist_hard_state(conn: &sled::Db, term: u32, voted_for: Option<Uuid>) -> Result<(), Error> {
    let meta = conn.open_tree(RAFT_META_TREE)?;
    meta.insert(KEY_CURRENT_TERM, &term.to_be_bytes())?;
    match voted_for {
        Some(id) => {
            meta.insert(KEY_VOTED_FOR, id.as_bytes().as_slice())?;
        }
        None => {
            meta.remove(KEY_VOTED_FOR)?;
        }
    };
    Ok(())
}

fn persist_log_entry(conn: &sled::Db, log: &Log) -> Result<(), Error> {
    let tree = conn.open_tree(RAFT_LOG_TREE)?;
    let persisted = PersistedLog {
        index: log.index,
        term: log.term,
        action: log.action.clone(),
    };
    tree.insert(log.index.to_be_bytes(), bincode::serialize(&persisted)?)?;
    Ok(())
}

fn truncate_log_from(conn: &sled::Db, from_index: u32) -> Result<(), Error> {
    let tree = conn.open_tree(RAFT_LOG_TREE)?;
    let keys: Vec<_> = tree
        .range(from_index.to_be_bytes()..)
        .keys()
        .collect::<Result<Vec<_>, _>>()?;
    for key in keys {
        tree.remove(key)?;
    }
    Ok(())
}

fn persist_last_applied(conn: &sled::Db, last_applied: u32) -> Result<(), Error> {
    let meta = conn.open_tree(RAFT_META_TREE)?;
    meta.insert(KEY_LAST_APPLIED, &last_applied.to_be_bytes())?;
    Ok(())
}

fn load_or_create_id(conn: &sled::Db) -> Result<Uuid, Error> {
    let meta = conn.open_tree(RAFT_META_TREE)?;
    if let Some(bytes) = meta.get(KEY_NODE_ID)? {
        return Uuid::from_slice(bytes.as_ref()).map_err(|_| Error::DeserializeFailed);
    }
    let id = Uuid::new_v4();
    meta.insert(KEY_NODE_ID, id.as_bytes().as_slice())?;
    conn.flush()?;
    Ok(id)
}

fn load_raft_state(conn: &sled::Db) -> Result<(u32, Option<Uuid>, Vec<Log>, u32), Error> {
    let meta = conn.open_tree(RAFT_META_TREE)?;

    let current_term = meta
        .get(KEY_CURRENT_TERM)?
        .map(|b| u32::from_be_bytes(b.as_ref().try_into().unwrap_or([0; 4])))
        .unwrap_or(0);

    let voted_for = meta
        .get(KEY_VOTED_FOR)?
        .and_then(|b| Uuid::from_slice(b.as_ref()).ok());

    let last_applied = meta
        .get(KEY_LAST_APPLIED)?
        .map(|b| u32::from_be_bytes(b.as_ref().try_into().unwrap_or([0; 4])))
        .unwrap_or(0);

    let log_tree = conn.open_tree(RAFT_LOG_TREE)?;
    let mut log = Vec::new();
    for result in log_tree.iter() {
        let (_, value) = result?;
        let persisted: PersistedLog = bincode::deserialize(&value)?;
        log.push(Log {
            index: persisted.index,
            term: persisted.term,
            action: persisted.action,
            committed: Arc::new((Mutex::new(true), Notify::new())),
        });
    }

    Ok((current_term, voted_for, log, last_applied))
}

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

/// The current role of a node in the Raft protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ElectionState {
    /// The node is campaigning for leadership.
    Candidate,
    /// The node is the current cluster leader and accepts writes.
    Leader,
    /// The node is a follower of the leader identified by the inner [`Uuid`].
    /// A `Uuid::nil()` means no leader has been elected yet.
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

/// A single node in a skiff cluster.
///
/// Construct one with [`Builder`](crate::Builder) and call [`start`](Skiff::start)
/// to begin serving requests.  The node spawns background tasks for leader
/// election and heartbeat management; call [`shutdown`](Skiff::shutdown) before
/// dropping to allow those tasks and the sled database to close cleanly.
#[derive(Debug, Clone)]
pub struct Skiff {
    id: Uuid,
    address: Ipv4Addr,
    port: u16,
    state: Arc<Mutex<State>>,
    tx_entries: Arc<Mutex<mpsc::Sender<u8>>>,
    rx_entries: Arc<Mutex<mpsc::Receiver<u8>>>,
    subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<SubscribeReply>>>>>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Skiff {
    pub(crate) fn new(
        address: Ipv4Addr,
        port: u16,
        data_dir: String,
        peers: Vec<Ipv4Addr>,
    ) -> Result<Self, Error> {
        let conn = sled::open(data_dir)?;
        let id = load_or_create_id(&conn)?;
        let (tx_entries, rx_entries) = mpsc::channel(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (current_term, voted_for, persisted_log, last_applied) = load_raft_state(&conn)?;

        // The index-0 entry is an in-memory sentinel carrying the bootstrap cluster config derived
        // from the peers argument. Persisted log entries (index >= 1) are appended on top and take
        // precedence in get_cluster() via rev().find(), so the latest Configure entry always wins.
        let mut cluster: HashMap<Uuid, Ipv4Addr> = peers
            .into_iter()
            .map(|addr| (Uuid::new_v4(), addr))
            .collect();

        cluster.insert(id, address);

        let mut log = vec![Log {
            term: 0,
            index: 0,
            action: Action::Configure(cluster),
            committed: Arc::new((Mutex::new(true), Notify::new())),
        }];
        log.extend(persisted_log);

        Ok(Skiff {
            id,
            address,
            port,
            state: Arc::new(Mutex::new(State {
                election_state: ElectionState::Follower(Uuid::nil()),
                current_term,
                voted_for,
                // Start conservative: committed_index catches up via leader heartbeats
                committed_index: last_applied,
                last_applied,
                peer_clients: HashMap::new(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                log,
                conn,
            })),
            tx_entries: Arc::new(Mutex::new(tx_entries)),
            rx_entries: Arc::new(Mutex::new(rx_entries)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
        })
    }

    // todo: initializing a new cluster with a known config without needing to send add_server rpc

    /// Return the stable, persistent UUID that identifies this node.
    ///
    /// The ID is generated on first startup and stored in sled so it survives
    /// restarts.
    pub fn get_id(&self) -> Uuid {
        self.id
    }

    /// Signal the node to stop its background tasks and close the gRPC server.
    ///
    /// This must be called before dropping the node in tests or applications
    /// that restart nodes, because sled holds a file lock that is only released
    /// once every `Arc` clone of the database handle has been dropped.
    /// `shutdown` triggers a clean teardown so those clones are released
    /// promptly.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Return the IPv4 address this node is bound to.
    pub fn get_address(&self) -> Ipv4Addr {
        self.address
    }

    /// Return `true` if the cluster has an active leader.
    ///
    /// This is `true` when this node is the leader, or when it is a follower
    /// that has acknowledged a specific leader.  It is `false` during initial
    /// startup or while an election is in progress.
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

    /// Block until the cluster has an elected leader, or until `timeout` elapses.
    ///
    /// Polls [`is_leader_elected`](Skiff::is_leader_elected) every 50 ms.
    /// Call this after spawning [`start`](Skiff::start) to ensure the cluster
    /// is ready before connecting a client.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LeaderElectionTimeout`] if no leader is elected within
    /// `timeout`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use skiff_rs::Builder;
    /// use std::time::Duration;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let node = Builder::new()
    ///     .set_dir("/tmp/my-node")
    ///     .bind("127.0.0.1".parse()?)
    ///     .build()?;
    ///
    /// let node_ref = node.clone();
    /// tokio::spawn(async move { node_ref.start().await });
    ///
    /// // Block until a leader is elected before connecting a client.
    /// node.wait_for_leader(Duration::from_secs(2)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<(), Error> {
        tokio::time::timeout(timeout, async {
            loop {
                if self.is_leader_elected().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| Error::LeaderElectionTimeout)
    }

    /// Return the current cluster membership as a map of node ID → address.
    ///
    /// The membership is derived from the most recent `Configure` entry in the
    /// Raft log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingClusterConfig`] if no configuration entry is
    /// found (this should not happen under normal operation).
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
            SocketAddrV4::new(*peers.get(peer).unwrap(), self.port)
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

    /// Return the node's current [`ElectionState`].
    pub async fn get_election_state(&self) -> ElectionState {
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

    async fn commit_logs(&self) -> Result<(), Error> {
        let committed_index = self.state.lock().await.committed_index;
        let last_applied = self.state.lock().await.last_applied;
        if committed_index <= last_applied {
            return Ok(());
        }

        trace!("committing log entries");
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
                    let tree_name =
                        match prefix.len() {
                            0 => "base".to_string(),
                            _ => {
                                let _ = self.state.lock().await.conn.update_and_fetch(
                                    "trees",
                                    |trees| match trees {
                                        Some(tree_vec) => {
                                            let mut updated_tree_vec =
                                                bincode::deserialize::<Vec<String>>(tree_vec)
                                                    .unwrap();

                                            if !updated_tree_vec.contains(&prefix) {
                                                updated_tree_vec.push(prefix.clone());
                                            }

                                            Some(bincode::serialize(&updated_tree_vec).unwrap())
                                        }
                                        None => Some(bincode::serialize(&vec![&prefix]).unwrap()),
                                    },
                                );

                                format!("base_{}", &prefix.replace("/", "_"))
                            }
                        };

                    // Todo: maybe alert subscribers on delete
                    let mut subscribers = self.subscribers.lock().await;
                    for (sub_prefix, senders) in subscribers.iter_mut() {
                        // Todo: this could cause issues when the provided prefix is parent/ and tree is ex. parents/ or parent1/, etc
                        if prefix.starts_with(sub_prefix.trim_end_matches("/")) {
                            let mut live_senders = Vec::new();
                            for sender in senders.drain(..) {
                                if sender
                                    .send(SubscribeReply {
                                        key: full_key.clone(),
                                        action: skiff_proto::Action::Insert as i32,
                                        value: Some(value.clone()),
                                    })
                                    .await
                                    .is_ok()
                                {
                                    live_senders.push(sender);
                                } // else: receiver dropped, discard sender
                            }
                            *senders = live_senders;
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
                            let _ = lock.conn.update_and_fetch("trees", |trees| match trees {
                                Some(tree_vec) => {
                                    let mut updated_tree_vec =
                                        bincode::deserialize::<Vec<String>>(tree_vec).unwrap();

                                    trace!(trees = ?updated_tree_vec, name = %tree_name, "dropping tree");
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

        let conn = self.state.lock().await.conn.clone();
        persist_last_applied(&conn, committed_index)?;
        conn.flush_async().await.map_err(Error::SledError)?;
        self.state.lock().await.last_applied = committed_index;

        Ok(())
    }

    async fn reset_heartbeat_timer(&self) {
        let _ = self.tx_entries.lock().await.send(1).await; // Can be anything
    }

    fn initialize_service(&self) -> SkiffServer<Skiff> {
        let skiff = self.clone();
        drop(tokio::spawn(async move {
            // Todo: when we're restoring a cluster from previous operation (ex. after outage / migration)
            // the cluster len for all nodes will be > 1, but no leader exists yet, so add_server rpc fails.
            // Might be ok if leader election happens subsequently, but this should a) be verified
            // and b) logic for this scenario should be made obvious
            if skiff.get_cluster().await.unwrap().len() > 1 {
                debug!("joining cluster");

                let mut joined_cluster = false;
                for id in skiff.get_peers().await.keys() {
                    debug!(?id, "asking peer to add us to cluster");

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
                    return Err(Error::ClusterJoinFailed);
                }
            }
            // Todo: if we are lone node in cluster we can skip election timeout and make ourselves leader

            let mut server1 = skiff.clone();
            let _elections: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                server1.election_manager().await?;
                Ok(())
            });

            Ok(())
        }));

        SkiffServer::new(self.clone())
    }

    /// Start the node's gRPC server and block until [`shutdown`](Skiff::shutdown) is called.
    ///
    /// This method must be called (usually inside a `tokio::spawn`) for the
    /// node to participate in the cluster.  It:
    ///
    /// 1. Spawns a background task that joins the cluster (if peers were
    ///    provided) and then runs the election/heartbeat loop.
    /// 2. Binds a tonic gRPC server on the configured address and port.
    /// 3. Returns only after [`shutdown`](Skiff::shutdown) is called and all
    ///    in-flight connections have been closed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RPCBindFailed`] if the gRPC server cannot bind.
    pub async fn start(self) -> Result<(), Error> {
        let service = self.initialize_service();
        let mut shutdown = self.shutdown_rx.clone();

        let bind_address = SocketAddr::new(self.address.into(), self.port);
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_shutdown(bind_address, async move {
                // Wait for shutdown signal, then let tonic close all connections cleanly.
                let _ = shutdown.changed().await;
            })
            .await?;

        Ok(())
    }

    #[allow(unreachable_code)]
    async fn election_manager(&mut self) -> Result<(), Error> {
        let mut rng = StdRng::from_entropy();

        let mut rx_entries = self.rx_entries.lock().await;
        let mut shutdown = self.shutdown_rx.clone();

        loop {
            self.commit_logs().await?;

            if self.get_election_state().await == ElectionState::Leader {
                // todo: refactor this block:
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(75)) => {

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

                            let client_arc = match self.get_peer_client(peer).await {
                                Ok(c) => c,
                                Err(_) => { self.drop_peer_client(peer).await; continue; }
                            };
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
                    _ = shutdown.changed() => { return Ok(()); }
                }
            } else {
                tokio::select! {
                    Some(1) = rx_entries.recv() => {
                        continue;
                    }

                    _ = tokio::time::sleep(Duration::from_millis(rng.gen_range(150..300))) => {
                        debug!("election timeout, starting election");
                        self.run_election().await?;
                    }

                    _ = shutdown.changed() => { return Ok(()); }
                };
            }
        }

        Ok(())
    }

    async fn run_election(&self) -> Result<(), Error> {
        self.set_election_state(ElectionState::Candidate).await;
        self.increment_term().await;
        self.vote_for(Some(self.id)).await;
        let term = self.get_current_term().await;
        let state = self.get_election_state().await;
        debug!(?state, term, "starting election");

        let mut num_votes: u32 = 1; // including self

        for peer in self.get_peers().await.keys() {
            trace!(?peer, "requesting vote");

            let mut request = Request::new(VoteRequest {
                term: self.get_current_term().await,
                candidate_id: self.id.to_string(),
                last_log_index: self.get_last_log_index().await,
                last_log_term: self.get_last_log_term().await,
            });
            request.set_timeout(Duration::from_millis(300));

            let client = match self.get_peer_client(peer).await {
                Ok(c) => c,
                Err(_) => {
                    self.drop_peer_client(peer).await;
                    continue;
                }
            };
            let response = match client.lock().await.request_vote(request).await {
                Ok(response) => response.into_inner(),
                Err(_) => {
                    self.drop_peer_client(peer).await;
                    continue;
                }
            };

            if response.vote_granted {
                debug!(peer = ?&peer, "received vote");
                num_votes += 1;
            }
        }

        if num_votes > self.get_cluster().await?.len() as u32 / 2 {
            info!("elected leader");
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

                let client_arc = match self.get_peer_client(id).await {
                    Ok(c) => c,
                    Err(_) => {
                        self.drop_peer_client(id).await;
                        continue;
                    }
                };
                let mut client = client_arc.lock().await;
                if client.append_entry(request).await.is_err() {
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
        trace!("received vote request");

        let current_term = self.get_current_term().await;
        let voted_for = self.get_voted_for().await;
        let last_log_index = self.get_last_log_index().await;
        let last_log_term = self.get_last_log_term().await;
        let conn = self.state.lock().await.conn.clone();

        let vote_request = request.into_inner();

        let candidate_id = Uuid::from_str(&vote_request.candidate_id)
            .map_err(|_| Status::invalid_argument("invalid candidate id"))?;

        // Grant vote if:
        // - Candidate's term is higher (fresh term, voted_for resets), OR
        // - Same term and we haven't voted yet or already voted for this candidate
        let can_vote = vote_request.term > current_term
            || (vote_request.term == current_term
                && (voted_for.is_none() || voted_for == Some(candidate_id)));

        if can_vote
            && vote_request.last_log_index >= last_log_index
            && vote_request.last_log_term >= last_log_term
        {
            debug!(?candidate_id, "granting vote");

            // Persist hard state before responding
            persist_hard_state(&conn, vote_request.term, Some(candidate_id))
                .map_err(|_| Status::internal("failed to persist hard state"))?;
            conn.flush_async()
                .await
                .map_err(|_| Status::internal("failed to flush"))?;

            self.vote_for(Some(candidate_id)).await;
            self.set_election_state(ElectionState::Follower(candidate_id))
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
        let entry_request = request.into_inner();
        let current_term = self.get_current_term().await;
        let conn = self.state.lock().await.conn.clone();

        if entry_request.term < current_term {
            return Ok(Response::new(EntryReply {
                term: current_term,
                success: false,
            }));
        }

        // Update term and clear voted_for if the leader has a newer term
        let term_changed = entry_request.term > current_term;
        if term_changed {
            self.set_current_term(entry_request.term).await;
            self.vote_for(None).await;
        }

        // Confirmed that we're receiving requests from a verified leader
        self.set_election_state(ElectionState::Follower(
            Uuid::from_str(&entry_request.leader_id).unwrap(),
        ))
        .await;
        self.reset_heartbeat_timer().await;

        if entry_request.prev_log_index > 0 {
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
                    term: entry_request.term,
                    success: false,
                }));
            }
        }

        for new_log in &entry_request.entries {
            let mut drop_index: Option<u32> = None;
            for current_log in &self.state.lock().await.log {
                // Conflict: same index but different term — truncate from here
                if current_log.index == new_log.index && current_log.term != new_log.term {
                    drop_index = Some(current_log.index);
                }
            }

            if let Some(drop_index) = drop_index {
                self.state
                    .lock()
                    .await
                    .log
                    .retain(|log| log.index < drop_index);
                truncate_log_from(&conn, drop_index)
                    .map_err(|_| Status::internal("failed to truncate log"))?;
            }
        }

        let last_log_index = self.get_last_log_index().await;
        let new_term = entry_request.term;
        let mut appended_entries = false;

        for new_log in entry_request.entries {
            if new_log.index > last_log_index {
                trace!("appending log entry");
                let log_entry = Log {
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
                };
                persist_log_entry(&conn, &log_entry)
                    .map_err(|_| Status::internal("failed to persist log entry"))?;
                self.state.lock().await.log.push(log_entry);
                appended_entries = true;
            }
        }

        // Flush to stable storage before responding — required by Raft safety
        if term_changed || appended_entries {
            if term_changed {
                persist_hard_state(&conn, new_term, None)
                    .map_err(|_| Status::internal("failed to persist hard state"))?;
            }
            conn.flush_async()
                .await
                .map_err(|_| Status::internal("failed to flush"))?;
        }

        if entry_request.leader_commit > self.get_commit_index().await {
            self.state.lock().await.committed_index =
                min(entry_request.leader_commit, self.get_last_log_index().await);
        }

        Ok(Response::new(EntryReply {
            term: new_term,
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

        debug!("adding server to cluster");
        let new_server = request.into_inner();
        let new_uuid = Uuid::from_str(&new_server.id).unwrap();

        let mut cluster_config: HashMap<Uuid, Ipv4Addr> = self.get_cluster().await.unwrap();

        let id = Uuid::from_str(&new_server.id).unwrap();
        let addr = Ipv4Addr::from_str(&new_server.address).unwrap();

        if let std::collections::hash_map::Entry::Vacant(e) = cluster_config.entry(id) {
            e.insert(addr);
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

    async fn remove_server(
        &self,
        request: Request<ServerRequest>,
    ) -> Result<Response<ServerReply>, Status> {
        let election_state = self.state.lock().await.election_state.clone();
        if let ElectionState::Follower(leader) = election_state {
            let client = self.get_peer_client(&leader).await;
            if let Ok(client_inner) = client {
                return client_inner.lock().await.remove_server(request).await;
            }
            return Err(Status::internal("failed to forward request to leader"));
        }

        let remove_request = request.into_inner();
        let remove_id = Uuid::from_str(&remove_request.id)
            .map_err(|_| Status::invalid_argument("invalid server id"))?;

        let mut cluster_config = self
            .get_cluster()
            .await
            .map_err(|_| Status::internal("failed to get cluster config"))?;

        if cluster_config.remove(&remove_id).is_none() {
            return Err(Status::not_found("server not found in cluster"));
        }

        self.log(Action::Configure(cluster_config.clone())).await;

        {
            let mut state = self.state.lock().await;
            state.next_index.remove(&remove_id);
            state.match_index.remove(&remove_id);
        }
        self.drop_peer_client(&remove_id).await;

        Ok(Response::new(ServerReply {
            success: true,
            cluster: Some(
                bincode::serialize(&cluster_config)
                    .map_err(|_| Status::internal("failed to serialize cluster"))?,
            ),
        }))
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

        let (_, notify) = &*commit_arc;
        tokio::time::timeout(Duration::from_secs(5), notify.notified())
            .await
            .map_err(|_| Status::deadline_exceeded("timed out waiting for commit"))?;
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

        let (_, notify) = &*commit_arc;
        tokio::time::timeout(Duration::from_secs(5), notify.notified())
            .await
            .map_err(|_| Status::deadline_exceeded("timed out waiting for commit"))?;
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
