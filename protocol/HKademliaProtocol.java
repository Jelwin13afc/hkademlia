import peersim.core.*;
import peersim.config.*;
import java.util.*;


public class HKademliaProtocol implements Protocol {
    private final int kadK;
    private final int kadA;
    private int clusterID;
    private Set<Node> kbucket;

    private final String prefix;

    private final Set<Long> localStore = new HashSet<>();

    private int cacheSize;
    private int cacheHits = 0;
    private int cacheMisses = 0;

    private static final int DEFAULT_CACHE_SIZE = 500;
    private static final String PAR_CACHE_SIZE = "cache_size";

    // Map to track content to its originating cluster
    private Map<String, Integer> contentOriginCluster;

    // LFU Cache implementation
    private LFUCache contentCache;

    private int interClusterStoreMessages = 0;
    private int intraClusterStoreMessages = 0;

    private int interClusterLookupMessages = 0;
    private int intraClusterLookupMessages = 0;

    public HKademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();

        this.cacheSize = Configuration.getInt(prefix + "." + PAR_CACHE_SIZE, DEFAULT_CACHE_SIZE);

        // // FIFO strategy for cache
        // this.contentCache = new LinkedHashMap<String, Object>(cacheSize, 0.75f, false) {
        //     @Override
        //     protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
        //         return size() > cacheSize;
        //     }
        // };

        // Create LFU cache with specified size
        this.contentCache = new LFUCache(cacheSize);

        // Track which cluster each content originated from
        this.contentOriginCluster = new HashMap<>();
    }

    // LFU Cache Implementation
    private static class LFUCache {
        private final int capacity;
        private final HashMap<String, Object> cache; // stores the actual key-value pairs
        private final HashMap<String, Integer> frequencies; // tracks access frequency for each key
        private final HashMap<Integer, LinkedHashSet<String>> frequencyLists; // groups keys by frequency
        private int minFrequency; // keeps track of the minimum frequency

        public LFUCache(int capacity) {
            this.capacity = capacity;
            this.cache = new HashMap<>();
            this.frequencies = new HashMap<>();
            this.frequencyLists = new HashMap<>();
            this.minFrequency = 0;
        }

        public Object get(String key) {
            if (!cache.containsKey(key)) {
                return null;
            }
            
            // Update frequency
            int oldFrequency = frequencies.get(key);
            frequencies.put(key, oldFrequency + 1);
            
            // Remove key from old frequency list
            frequencyLists.get(oldFrequency).remove(key);
            
            // Update minFrequency if needed
            if (minFrequency == oldFrequency && frequencyLists.get(oldFrequency).isEmpty()) {
                minFrequency = oldFrequency + 1;
            }
            
            // Add key to new frequency list
            frequencyLists.computeIfAbsent(oldFrequency + 1, k -> new LinkedHashSet<>()).add(key);
            
            return cache.get(key);
        }

        public void put(String key, Object value) {
            if (capacity <= 0) {
                return;
            }
            
            // If key exists, update its value and frequency
            if (cache.containsKey(key)) {
                cache.put(key, value);
                get(key); // update frequency
                return;
            }
            
            // If cache is full, remove least frequently used item
            if (cache.size() >= capacity) {
                String leastFrequentKey = frequencyLists.get(minFrequency).iterator().next();
                frequencyLists.get(minFrequency).remove(leastFrequentKey);
                cache.remove(leastFrequentKey);
                frequencies.remove(leastFrequentKey);
            }
            
            // Add new key with frequency 1
            cache.put(key, value);
            frequencies.put(key, 1);
            minFrequency = 1;
            frequencyLists.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(key);
        }

        public boolean containsKey(String key) {
            return cache.containsKey(key);
        }

        public void clear() {
            cache.clear();
            frequencies.clear();
            frequencyLists.clear();
            minFrequency = 0;
        }

        public int size() {
            return cache.size();
        }
        
        public Set<String> keySet() {
            return cache.keySet();
        }
    }

    // The clone() method ensures that each peer gets a new instance of your protocol class
    public Object clone(){
        return new HKademliaProtocol(prefix);
    }

    public void addPeer(Node selfNode, Node peer) {
        // Apply H-Kademlia KBucket insertion rules
        // get the protocol
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);  // Extract "hkademlia"
        int pid = Configuration.lookupPid(protocolId);
        HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);
        // get the cluster Id
        int peerClusterId = peerProtocol.getClusterId();

        //  check if peer is local, if so always add to cluster
        if (peerClusterId == this.clusterID) {

            kbucket.add(peer);
            long peerId = peer.getID();
            long selfId = selfNode.getID();

            // Remove any remote peers that are farther from the new peer than this node is
            kbucket.removeIf(other -> {
                HKademliaProtocol otherProtocol = (HKademliaProtocol) other.getProtocol(pid);
                boolean isRemote = otherProtocol.getClusterId() != this.clusterID;
                long otherDistance = xorDistance(other.getID(), peerId);
                long selfDistance = xorDistance(selfId, peerId);
                return isRemote && otherDistance > selfDistance;
            });
        }
        else{
            Node closestInCluster = getClosestPeerInCluster(peer.getID(), pid);
            if (closestInCluster != null && closestInCluster.getID() == selfNode.getID()) {
                // Become gateway peer
                kbucket.add(peer);
            }
        }
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public void executeStore(long contentId) {
        // Simulate STORE action based on contentId and protocol
        
        
        String contentIdStr = String.valueOf(contentId);
        localStore.add(contentId); // Storing in this node as well, may be optional
        Object content = "Content-" + contentIdStr;

        // find kadk number of closest peers,contenID to store 
        List<Node> closestPeers = findClosestPeers(contentId, kadK);

        // Store content in local cache first
        storeInCache(contentIdStr, content);

        String protocolId = prefix.substring(prefix.lastIndexOf('.')+1);
        int pid = Configuration.lookupPid(protocolId);
        for (Node peer : closestPeers) {
            HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);
            peerProtocol.localStore.add(contentId);

            // Track inter/intra-cluster messages
            if (peerProtocol.getClusterId() == this.getClusterId()) {
                intraClusterStoreMessages++;
            } else {
                interClusterStoreMessages++;
            }

            // Simulate storing content on that peer (abstract logic)
            System.out.println("Storing content " + contentId + " on peer " + peer.getID());
        }
    }

    public HKademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        // Simulate LOOKUP action based on contentId

        // first check local cache
        String contentIdStr = String.valueOf(contentId);
        Object cachedContent = searchCache(contentIdStr);
        if (cachedContent != null) {
            // Cache hit - return result immediately with 0 hops
            cacheHits++;
            System.out.println("Cache hit for content " + contentId);
            return new HKademliaStoreLookupSimulator.LookupResult(true, 0, 0);
        }
        // Not in local cache
        cacheMisses++;
        // Next check local store
        if (localStore.contains(contentId)) {
            return new HKademliaStoreLookupSimulator.LookupResult(true, 0, 0);
        }
        // Nodes that we've already contacted
        Set<Node> contacted = new HashSet<>();
        // Custom priority queue based on shortest distances
        PriorityQueue<Node> shortestDistances = new PriorityQueue<>(
                Comparator.comparingLong(n -> xorDistance(n.getID(), contentId))
        );
        // Find kadA closest peers
        shortestDistances.addAll(findClosestPeers(contentId, kadA));
        int hops = 0;
        long latency = 0;
        boolean success = false;
        String protocolId = prefix.substring(prefix.lastIndexOf('.')+1);
        int pid = Configuration.lookupPid(protocolId);

        while(!shortestDistances.isEmpty()) {
            List<Node> newPeers = new ArrayList<>(kadA);
            Iterator<Node> iterator = shortestDistances.iterator();
            while(iterator.hasNext() && newPeers.size() < kadA) {
                // Find the next set of peers to go through
                Node n = iterator.next();
                if (!contacted.contains(n)){
                    newPeers.add(n);
                }
            }
            if (newPeers.isEmpty()) {
                break;
            }
            for (Node peer : newPeers) {
                contacted.add(peer);
                hops++;
                latency++; // Fix with real latency
                HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);

                if (peerProtocol.getClusterId() == this.getClusterId()) {
                    intraClusterLookupMessages++;
                } else {
                    interClusterLookupMessages++;
                }
                
                if (peerProtocol.localStore.contains(contentId)) {
                    success = true;
                    break;
                }
                if (peerProtocol.isCached(contentIdStr)) {
                    success = true;
                    break;
                }
                // Shortest distances from beginning peers to later
                shortestDistances.addAll(peerProtocol.findClosestPeers(contentId, kadK));
            }
            if (success) {
                break;
            }
        }

        return new HKademliaStoreLookupSimulator.LookupResult(success, hops, latency);
//        return null;
    }

    public void setClusterId(int id) {
        this.clusterID = id;
    }

    public int getClusterId() {
        return clusterID;
    }

    private long xorDistance(long id1, long id2) {
        return id1 ^ id2;
    }

    private Node getClosestPeerInCluster(long targetId, int pid) {
        Node closest = null;
        long minDistance = Long.MAX_VALUE;
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            HKademliaProtocol proto = (HKademliaProtocol) node.getProtocol(pid);
            if (proto.getClusterId() == this.clusterID) {
                long distance = xorDistance(node.getID(), targetId);
                if (distance < minDistance) {
                    closest = node;
                    minDistance = distance;
                }
            }
        }
        return closest;
    }

    private List<Node> findClosestPeers(long targetId, int count) {
        PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingLong(n -> xorDistance(n.getID(), targetId)));
        pq.addAll(kbucket);
        List<Node> result = new ArrayList<>();
        while (!pq.isEmpty() && result.size() < count) {
            result.add(pq.poll());
        }
        return result;
    }

    // Register which cluster a content originated from
    public void registerContentOrigin(String contentId, int clusterId) {
        contentOriginCluster.put(contentId, clusterId);
    }

    // Get the origin cluster of a content
    public Integer getContentOriginCluster(String contentId) {
        return contentOriginCluster.get(contentId);
    }

    // store content in cache
    public void storeInCache(String contentId, Object content){
        contentCache.put(contentId, content);
    }

    // Search for content in local cache
    public Object searchCache(String contentId) {
        Object result = contentCache.get(contentId);
        
        // Update stats (optional)
        if (result != null) {
            cacheHits++;
        } else {
            cacheMisses++;
        }
        
        return result;
    }

    //Check if content exists in cache
    public boolean isCached(String contentId) {
        return contentCache.containsKey(contentId);
    }

    /**
     * Clear the entire cache
     */
    public void clearCache() {
        contentCache.clear();
    }

    /**
     * Get cache statistics
     * @return String representation of cache stats
     */
    public String getCacheStats() {
        int totalRequests = cacheHits + cacheMisses;
        double hitRatio = totalRequests > 0 ? (double)cacheHits / totalRequests : 0;
        
        return String.format("Cache size: %d/%d, Hits: %d, Misses: %d, Hit ratio: %.2f%%", 
                contentCache.size(), cacheSize, cacheHits, cacheMisses, hitRatio * 100);
    }

    public int getKBucketSize() {
        return kbucket.size();
    }

    public int getInterStoreMessages() { return interClusterStoreMessages; }
    public int getIntraStoreMessages() { return intraClusterStoreMessages; }

    public int getInterLookupMessages() { return interClusterLookupMessages; }
    public int getIntraLookupMessages() { return intraClusterLookupMessages; }

}
