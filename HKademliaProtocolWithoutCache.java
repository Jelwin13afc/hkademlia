


// Core Logic of HKademlia, how peers interact: routing,  KBucket updates, remote vs local peer logic
import peersim.core.*;
import peersim.config.*;
import java.util.*;

public class HKademliaProtocolWithoutCache implements Protocol {
    private final int kadK;
    private final int kadA;
    private int clusterID;
    private Set<Node> kbucket;

    private final String prefix;

    private final Set<Long> localStore = new HashSet<>();

    // Map to track content to its originating cluster
    private Map<String, Integer> contentOriginCluster;

    public HKademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();

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

    public void executeStore(long contentId) {
        // Simulate STORE action based on contentId and protocol
        
        
        String contentIdStr = String.valueOf(contentId);
        localStore.add(contentId); // Storing in this node as well, may be optional
        Object content = "Content-" + contentIdStr;

        // find kadk number of closest peers,contenID to store 
        List<Node> closestPeers = findClosestPeers(contentId, kadK);

        String protocolId = prefix.substring(prefix.lastIndexOf('.')+1);
        int pid = Configuration.lookupPid(protocolId);
        for (Node peer : closestPeers) {
            HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);
            peerProtocol.localStore.add(contentId);
            // Simulate storing content on that peer (abstract logic)
            System.out.println("Storing content " + contentId + " on peer " + peer.getID());
        }
    }

    public HKademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        // Simulate LOOKUP action based on contentId

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

    
}
