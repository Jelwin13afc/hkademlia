// Core Logic of Kademlia Protocol
import peersim.core.*;
import peersim.config.*;
import java.util.*;

public class KademliaProtocol implements Protocol {
    private final int kadK;
    private final int kadA;
    private Set<Node> kbucket;

    private final String prefix;

    private final Set<Long> localStore = new HashSet<>();

    private int cacheSize;
    private int cacheHits = 0;
    private int cacheMisses = 0;

    private static final int DEFAULT_CACHE_SIZE = 500;
    private static final String PAR_CACHE_SIZE = "cache_size";

    private LinkedHashMap<String, Object> contentCache;

    // Map to track content to its originating cluster (for metrics only)
    private Map<String, Integer> contentOriginCluster;
    private int clusterID; // Needed for consistent metric calculation

    private int intraClusterStore = 0;
    private int interClusterStore = 0;
    private int intraClusterLookup = 0;
    private int interClusterLookup = 0;

    public KademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();

        this.cacheSize = Configuration.getInt(prefix + "." + PAR_CACHE_SIZE, DEFAULT_CACHE_SIZE);

        // FIFO strategy for cache
        this.contentCache = new LinkedHashMap<String, Object>(cacheSize, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                return size() > cacheSize;
            }
        };

        // Track which cluster each content originated from (for metrics)
        this.contentOriginCluster = new HashMap<>();
    }

    // The clone() method ensures that each peer gets a new instance of your protocol class
    public Object clone(){
        return new KademliaProtocol(prefix);
    }

    public void addPeer(Node selfNode, Node peer) {
        long selfId = selfNode.getID();
        long peerId = peer.getID();
        long distance = xorDistance(selfId, peerId);

        // Directly add to k-bucket, no clustering logic here
        kbucket.add(peer);

        // Maintain k-bucket size
        if (kbucket.size() > kadK) {
            // Sort k-bucket by distance to self and remove the farthest
            List<Node> sortedBucket = new ArrayList<>(kbucket);
            sortedBucket.sort(Comparator.comparingLong(n -> xorDistance(n.getID(), selfId)));
            kbucket = new HashSet<>(sortedBucket.subList(0, kadK));
        }
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public KademliaStoreLookupSimulator.StoreResult executeStore(long contentId) {
        String contentIdStr = String.valueOf(contentId);
        localStore.add(contentId);
        storeInCache(contentIdStr, "Content-" + contentIdStr);

        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int sourceClusterId = this.getClusterId(); // For metrics

        Set<Node> contacted = new HashSet<>();
        List<Node> closestNodes = findClosestPeers(contentId, kadK);
        PriorityQueue<Node> candidates = new PriorityQueue<>(
                Comparator.comparingLong(n -> xorDistance(n.getID(), contentId))
        );
        candidates.addAll(closestNodes);

        int hops = 0;
        long latency = 0;
        boolean changed = true;
        int receivers = 0; // Added to track actual number of receivers
        int localIntraMessages = 0;
        int localInterMessages = 0;

        while (changed && !candidates.isEmpty()) {
            changed = false;
            List<Node> alphaSet = new ArrayList<>();

            // Select next kadA peers to contact
            while (!candidates.isEmpty() && alphaSet.size() < kadA) {
                Node n = candidates.poll();
                if (!contacted.contains(n)) {
                    alphaSet.add(n);
                    contacted.add(n);
                }
            }

            if (alphaSet.isEmpty()) break;
            hops++;

            // Calculate latency for this hop (using cluster info for metrics)
            long maxHopLatency = 0;
            for (Node node : alphaSet) {
                long hopLatency = calculateLatency(Network.get((int)CommonState.getNode().getID()), node);
                maxHopLatency = Math.max(maxHopLatency, hopLatency);
            }
            latency += maxHopLatency;

            // Process responses
            for (Node node : alphaSet) {
                KademliaProtocol peerProto = (KademliaProtocol) node.getProtocol(pid);
                Node selfNode = getSelfNode(pid);
                int peerClusterId = peerProto.getClusterId();

                if (peerClusterId == sourceClusterId) {
                    localIntraMessages++;
                } else {
                    localInterMessages++;
                }

                List<Node> neighbors = peerProto.findClosestPeers(contentId, kadK);

                for (Node neighbor : neighbors) {
                    if (!contacted.contains(neighbor)) {
                        candidates.add(neighbor);
                    }
                }

                // Update closest nodes
                for (Node n : neighbors) {
                    if (!closestNodes.contains(n)) {
                        closestNodes.add(n);
                        changed = true;
                        this.addPeer(selfNode, n);
                    }
                }

                // Keep only kadK closest
                closestNodes.sort(Comparator.comparingLong(n -> xorDistance(n.getID(), contentId)));
                if (closestNodes.size() > kadK) {
                    closestNodes = closestNodes.subList(0, kadK);
                }
            }
        }

        // Store content on final kadK closest peers and count actual receivers
        for (Node node : closestNodes) {
            KademliaProtocol proto = (KademliaProtocol) node.getProtocol(pid);
            int peerClusterId = proto.getClusterId();

            if (peerClusterId == sourceClusterId) {
                localIntraMessages++;
            } else {
                localInterMessages++;
            }

            proto.localStore.add(contentId);
            receivers++; // Count each successful store
        }

        // Ensure we don't exceed kadK
        receivers = Math.min(receivers, kadK);

        this.intraClusterStore += localIntraMessages;
        this.interClusterStore += localInterMessages;

        return new KademliaStoreLookupSimulator.StoreResult(hops, latency, receivers, localIntraMessages, localInterMessages);
    }


    public KademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        // Simulate LOOKUP action based on contentId

        // first check local cache
        String contentIdStr = String.valueOf(contentId);
        Object cachedContent = searchCache(contentIdStr);
        if (cachedContent != null) {
            // Cache hit - return result immediately with 0 hops
            cacheHits++;
            return new KademliaStoreLookupSimulator.LookupResult(true, 0, 0, 0, 0);
        }
        // Not in local cache
        cacheMisses++;
        // Next check local store
        if (localStore.contains(contentId)) {
            return new KademliaStoreLookupSimulator.LookupResult(true, 0, 0, 0, 1);
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
        int lookupInterMessages = 0;
        int lookupIntraMessages = 0;
        int sourceClusterId = this.getClusterId(); // For metrics

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
                KademliaProtocol peerProtocol = (KademliaProtocol) peer.getProtocol(pid);
                int peerClusterId = peerProtocol.getClusterId();

                if (peerClusterId == sourceClusterId) {
                    lookupIntraMessages++;
                } else {
                    lookupInterMessages++;
                }

                if (peerProtocol.localStore.contains(contentId)) {
                    success = true;
                    break;
                }
                if (peerProtocol.contentCache.containsKey(contentId)) {
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

        this.intraClusterLookup += lookupIntraMessages;
        this.interClusterLookup += lookupInterMessages;

        return new KademliaStoreLookupSimulator.LookupResult(success, hops, latency, lookupIntraMessages, lookupInterMessages);
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

    private List<Node> findClosestPeers(long targetId, int count) {
        PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingLong(n -> xorDistance(n.getID(), targetId)));
        pq.addAll(kbucket);
        List<Node> result = new ArrayList<>();
        while (!pq.isEmpty() && result.size() < count) {
            result.add(pq.poll());
        }
        return result;
    }

    // Add this to your protocol class
    private long calculateLatency(Node from, Node to) {
        // Get cluster IDs (for metrics)
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int fromCluster = ((KademliaProtocol)from.getProtocol(pid)).getClusterId();
        int toCluster = ((KademliaProtocol)to.getProtocol(pid)).getClusterId();

        // Base latency values (ms) - adjust these based on your needs
        long intraClusterLatency = 5 + (long)(Math.random() * 5); // 5-10ms within cluster
        long interClusterLatency = 20 + (long)(Math.random() * 20); // 20-40ms between clusters

        return (fromCluster == toCluster) ? intraClusterLatency : interClusterLatency;
    }

    // Register which cluster a content originated from (for metrics)
    public void registerContentOrigin(String contentId, int clusterId) {
        contentOriginCluster.put(contentId, clusterId);
    }

    // Get the origin cluster of a content (for metrics)
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

    public int getKadK() {
        // Return the configured k-bucket size
        return this.kadK;
    }

    public int getIntraClusterStore() {
        return intraClusterStore;
    }
    public int getInterClusterStore() {
        return interClusterStore;
    }
    public int getIntraClusterLookup() {
        return intraClusterLookup;
    }
    public int getInterClusterLookup() {
        return interClusterLookup;
    }

    private Node getSelfNode(int pid) {
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            if (node.getProtocol(pid) == this) {
                return node;
            }
        }
        return null;
    }
}