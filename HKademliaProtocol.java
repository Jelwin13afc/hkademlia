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

    public HKademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();
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
        // find kadk number of closest peers,contenID to store 
        List<Node> closestPeers = findClosestPeers(contentId, kadK);
        for (Node peer : closestPeers) {
            // Simulate storing content on that peer (abstract logic)
            System.out.println("Storing content " + contentId + " on peer " + peer.getID());
        }
    }

    public HKademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        // Simulate LOOKUP action based on contentId

        return new HKademliaStoreLookupSimulator.LookupResult(success, hops, latency);
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

}
