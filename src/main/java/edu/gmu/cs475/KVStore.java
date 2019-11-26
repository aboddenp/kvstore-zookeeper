package edu.gmu.cs475;

import com.sun.corba.se.spi.ior.ObjectKey;
import edu.gmu.cs475.internal.IKVStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.DelegatingExecutorService;
import org.apache.zookeeper.CreateMode;
import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVStore extends AbstractKVStore {

    // how leaders are elected and how to find out who is the current one by calling leaderLatch.getLeader()
    private LeaderLatch leaderLatch;
    // a connection to the current leader
    private IKVStore leaderKVStoreConnection;
    // connections currently maintained by zookeeper
    private TreeCache connectedClients;
    // leader: hashmap where data is stored, follower: hashmap where a subset of the leaders data is cached
    private volatile ConcurrentHashMap<String, String> localCache = new ConcurrentHashMap<>();
    // hashmap mantained by leader to know who has cached what: key = string key and value = list of clients that have it cached
    private volatile ConcurrentHashMap<String, HashSet<String>> keys_cached_in_followers = new ConcurrentHashMap<>();
    // locks for reading and writing same key
    private volatile ConcurrentHashMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    // nodes that have been disconnected
    private volatile ConcurrentHashMap<String,String> disconnectedNodes = new ConcurrentHashMap<>();

    // flag to check if we are still connected
    private volatile boolean connected = true;
    private volatile String currentLeader = "";

    /**
     * This callback is invoked once your client has started up and published an RMI endpoint.
     * <p>
     * In this callback, you will need to set-up your ZooKeeper connections, and then publish your
     * RMI endpoint into ZooKeeper (publishing the hostname and port)
     * <p>
     * You will also need to set up any listeners to track ZooKeeper events
     *
     * @param localClientHostname Your client's hostname, which other clients will use to contact you
     * @param localClientPort     Your client's port number, which other clients will use to contact you
     */
    @Override

    public void initClient(String localClientHostname, int localClientPort) {
        // initialize membership and leaderlach for this new server:
        PersistentNode membershipNode = new PersistentNode(zk, CreateMode.EPHEMERAL, false, ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
        leaderLatch = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
        connectedClients = new TreeCache(zk, ZK_MEMBERSHIP_NODE); // Way to keep track of who is still connected to zoo keeper

        connectedClients.getListenable().addListener((client, event) -> {
//            System.out.println("client in " + getLocalConnectString() + " detected change in connected clients: " + event);
            if (event.getType().equals(TreeCacheEvent.Type.NODE_REMOVED) ) {
                disconnectedNodes.put(event.getData().getPath(),"");
            } else if (event.getType().equals(TreeCacheEvent.Type.NODE_ADDED) ) {
                disconnectedNodes.remove(event.getData().getPath());
            }
        });

        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                // became leader reset
                keys_cached_in_followers = new ConcurrentHashMap<>();
                disconnectedNodes = new ConcurrentHashMap<>();
            }

            @Override
            public void notLeader() {
                // Not the leader any more delete current information about cached data
                keys_cached_in_followers = new ConcurrentHashMap<>();
                disconnectedNodes = new ConcurrentHashMap<>();
            }
        });

        membershipNode.start();
        try {
            leaderLatch.start(); // puts this leaderlatch as a participant and a leader is elected if there is none
            connectedClients.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            while (leaderLatch.getLeader().getId().equals("")) { } // if there is no leader wait
            leaderKVStoreConnection = connectToKVStore(leaderLatch.getLeader().getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieve the value of a key
     *
     * @param key
     * @return The value of the key or null if there is no such key
     * @throws IOException if this client or the leader is disconnected from ZooKeeper
     */
    @Override
    public String getValue(String key) throws IOException {
        // if we lost connection to zk throw exception
        if (!connected) {// need to check connection here
            throw new IOException("client at " + getLocalConnectString() + "has disconnected from zookeeper");
        }

        // check if leader has changed since previous call to get safety maintained by method call
        fix_cache();

        // if we have the value cached return it
        if (localCache.containsKey(key)) {
            return localCache.get(key);
        }
        // if we do not have it cached we need to ask the leader for it

        // connect to the leader
        boolean got_connection_to_leader = false;
        while (!got_connection_to_leader) {
            try {
                leaderKVStoreConnection = connectToKVStore(leaderLatch.getLeader().getId());
                got_connection_to_leader = true;
            } catch (Exception e) {
            }
        }

        String value = leaderKVStoreConnection.getValue(key, getLocalConnectString());
        // cache the value for later calls to same key only if it is not null
        if (value != null) {
            localCache.put(key, value);
        }
        return value;

    }

    /**
     * Update the value of a key. After updating the value, this new value will be locally cached.
     *
     * @param key
     * @param value
     * @throws IOException if this client or the leader is disconnected from ZooKeeper
     */
    @Override
    public void setValue(String key, String value) throws IOException {

            // check that we have not lost connection to zk
            if (!connected) {// need to check connection here
                throw new IOException("client at " + getLocalConnectString() + "has disconnected from zookeeper");
            }

            // flush cache if necessary safety mantained by this method
            fix_cache();

            // connect to the leader
            boolean got_connection_to_leader = false;
            while (!got_connection_to_leader) {
                try {
                    leaderKVStoreConnection = connectToKVStore(leaderLatch.getLeader().getId());
                    got_connection_to_leader = true;
                } catch (Exception e) {
                }
            } // retry until we get a connection allows time for getting a new leader as well

            // ask leader to change the value of the key
            leaderKVStoreConnection.setValue(key, value, getLocalConnectString());
            // cache the value for later calls to same key
            localCache.put(key, value);
    }

    /**
     * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
     * <p>
     * This command should ONLY be called as a request to the leader.
     *
     * @param key    The key requested
     * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
     * @return The value of the key, or null if there is no value for this key
     * <p>
     * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
     */
    @Override
    public String getValue(String key, String fromID) throws RemoteException {
        // not necessary to pass Part 2 tests, but I added it for precaution
        // we should not be able to get and set on same key at the same time
        ReentrantReadWriteLock.ReadLock key_lock;
        synchronized (this) {
            if (locks.get(key) == null) {
                ReentrantReadWriteLock new_lock = new ReentrantReadWriteLock();
                locks.put(key, new_lock);
                key_lock = new_lock.readLock();
            } else {
                key_lock = locks.get(key).readLock();
            }
        }

        key_lock.lock();
        try {
            // this method is for the leader only: provide the value to the follower and maintain a record of this
            if (keys_cached_in_followers.containsKey(key)) {
                keys_cached_in_followers.get(key).add(fromID);
            } else if (localCache.containsKey(key)) { // if we have the key keep record that this follower has it cached
                keys_cached_in_followers.put(key, new HashSet<String>(Collections.singletonList(fromID)));
            }
        } finally {
            key_lock.unlock();
        }
        return localCache.get(key); // return the value at that key
    }

    /**
     * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
     * <p>
     * This command should ONLY be called as a request to the leader.
     * <p>
     * This command must wait for any pending writes on the same key to be completed
     *
     * @param key    The key to update
     * @param value  The new value
     * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
     */
    @Override
    public void setValue(String key, String value, String fromID) throws IOException {
        if (!connected){
            throw  new IOException("Leader not connected to zookeeper");
        }

        // mantaining safety cannot write same key at the same time
        ReentrantReadWriteLock.WriteLock key_lock;
        synchronized (this) {
            if (locks.get(key) == null) {
                ReentrantReadWriteLock new_lock = new ReentrantReadWriteLock();
                locks.put(key, new_lock);
                key_lock = new_lock.writeLock();
            } else {
                key_lock = locks.get(key).writeLock();
            }
        }

        key_lock.lock();
        try {
            // this method is for the leader only: provide the value to the follower and maintain a record of this

            if (keys_cached_in_followers.containsKey(key)) {  // someone else has this value cached notify all, flush old, add to new list
                HashSet<String> followers = keys_cached_in_followers.get(key);
                // send invalidate message to all followers that had this key cached
                while(!followers.isEmpty()){
                    ArrayList<String> contacted = new ArrayList<String>();
                    for (String id : followers) {
                        try {
                            if(disconnectedNodes.containsKey(ZK_MEMBERSHIP_NODE+"/"+id)){
                                contacted.add(id);
                            }else {
                                IKVStore follower_connection = connectToKVStore(id);
                                follower_connection.invalidateKey(key);
                                contacted.add(id);
                            }

                        } catch (Exception e) { }
                    }
                    followers.removeAll(contacted);
                }
            }
        } finally {
            key_lock.unlock();
        }
        // this follower is the only one that has it cached now
        keys_cached_in_followers.put(key, new HashSet<String>(Collections.singletonList(fromID)));
        localCache.put(key, value); // save the change to local cache

    }

    /**
     * Instruct a node to invalidate any cache of the specified key.
     * <p>
     * This method is called BY the LEADER, targeting each of the clients that has cached this key.
     *
     * @param key key to invalidate
     *            <p>
     *            DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
     */
    @Override
    public void invalidateKey(String key) throws RemoteException {
        // our way of invalidating the key is to remove it from our local cache
        synchronized (this) { // control multiple invalidates
            localCache.remove(key);
        }
    }

    /**
     * Called when ZooKeeper detects that your connection status changes
     *
     * @param curatorFramework
     * @param connectionState
     */
    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
//        System.out.println("Client in " + getLocalConnectString() + "changed connection state to: " + connectionState);
        if ((connectionState.equals(ConnectionState.RECONNECTED))) {
            connected = true;
            // check if there are others still connected
            if (connectedClients.getCurrentChildren(ZK_MEMBERSHIP_NODE).size() == 1) {
                // I am the only connected node and I can keep my cache
            } else {
                // there are more clients connected I need to flush the cache
                localCache = new ConcurrentHashMap<String, String>();
            }
        } else if ((connectionState.equals(ConnectionState.SUSPENDED))) {
            connected = false;
        }
    }

    private void fix_cache(){
        // check if leader has changed since previous call to get
        synchronized(this){ // avoid multiple threads modifyng each others current leader
            try {
                String syncLeader = leaderLatch.getLeader().getId();
                if (!(currentLeader.equals(syncLeader)) && !(syncLeader.equals(getLocalConnectString()))) {
                    // if leader changed and this client is not it, flush the cache
                    localCache = new ConcurrentHashMap<>();
                }
                currentLeader = syncLeader; // update who is the leader

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Release any ZooKeeper resources that you setup here
     * (The connection to ZooKeeper itself is automatically cleaned up for you)
     */
    @Override
    protected void _cleanup() {

    }
}


