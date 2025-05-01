// Core Logic of HKademlia, how peers interact: routing,  KBucket updates, remote vs local peer logic
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

    private LinkedHashMap<String, Object> contentCache;

    // Map to track content to its originating cluster
    private Map<String, Integer> contentOriginCluster;

    public HKademliaProtocol(String prefix) {
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

        // Track which cluster each content originated from
        this.contentOriginCluster = new HashMap<>();
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

    public HKademliaStoreLookupSimulator.StoreResult executeStore(long contentId) {
        String contentIdStr = String.valueOf(contentId);
        localStore.add(contentId);
        storeInCache(contentIdStr, "Content-" + contentIdStr);

        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);

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
            
            // Calculate latency for this hop
            long maxHopLatency = 0;
            for (Node node : alphaSet) {
                long hopLatency = calculateLatency(Network.get((int)CommonState.getNode().getID()), node);
                maxHopLatency = Math.max(maxHopLatency, hopLatency);
            }
            latency += maxHopLatency;

            // Process responses
            for (Node node : alphaSet) {
                HKademliaProtocol peerProto = (HKademliaProtocol) node.getProtocol(pid);
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
            HKademliaProtocol proto = (HKademliaProtocol) node.getProtocol(pid);
            proto.localStore.add(contentId);
            receivers++; // Count each successful store
        }

        // Ensure we don't exceed kadK
        receivers = Math.min(receivers, kadK);
        
        return new HKademliaStoreLookupSimulator.StoreResult(hops, latency, receivers);
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

    // Add this to your protocol class
    private long calculateLatency(Node from, Node to) {
        // Get cluster IDs
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int fromCluster = ((HKademliaProtocol)from.getProtocol(pid)).getClusterId();
        int toCluster = ((HKademliaProtocol)to.getProtocol(pid)).getClusterId();
        
        // Base latency values (ms) - adjust these based on your needs
        long intraClusterLatency = 5 + (long)(Math.random() * 5); // 5-10ms within cluster
        long interClusterLatency = 20 + (long)(Math.random() * 20); // 20-40ms between clusters
        
        return (fromCluster == toCluster) ? intraClusterLatency : interClusterLatency;
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

}