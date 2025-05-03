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

    private final Map<String, Integer> contentOriginCluster;

    private int intraClusterStore = 0;
    private int interClusterStore = 0;
    private int intraClusterLookup = 0;
    private int interClusterLookup = 0;

    public HKademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();
        this.contentOriginCluster = new HashMap<>();
    }

    public Object clone() {
        return new HKademliaProtocol(prefix);
    }

    public void addPeer(Node selfNode, Node peer) {
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);
        int peerClusterId = peerProtocol.getClusterId();

        if (peerClusterId == this.clusterID) {
            kbucket.add(peer);
            long peerId = peer.getID();
            long selfId = selfNode.getID();
            kbucket.removeIf(other -> {
                HKademliaProtocol otherProtocol = (HKademliaProtocol) other.getProtocol(pid);
                boolean isRemote = otherProtocol.getClusterId() != this.clusterID;
                long otherDistance = xorDistance(other.getID(), peerId);
                long selfDistance = xorDistance(selfId, peerId);
                return isRemote && otherDistance > selfDistance;
            });
        } else {
            Node closestInCluster = getClosestPeerInCluster(peer.getID(), pid);
            if (closestInCluster != null && closestInCluster.getID() == selfNode.getID()) {
                kbucket.add(peer);
            }
        }
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public HKademliaStoreLookupSimulator.StoreResult executeStore(long contentId) {
        localStore.add(contentId);

        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);

        Set<Node> contacted = new HashSet<>();
        List<Node> closestNodes = findClosestPeers(contentId, kadK);
        PriorityQueue<Node> candidates = new PriorityQueue<>(Comparator.comparingLong(n -> xorDistance(n.getID(), contentId)));
        candidates.addAll(closestNodes);

        int hops = 0;
        long latency = 0;
        boolean changed = true;
        int receivers = 0;

        int localIntraMessages = 0;
        int localInterMessages = 0;

        int sourceClusterId = this.getClusterId();

        while (changed && !candidates.isEmpty()) {
            changed = false;
            List<Node> alphaSet = new ArrayList<>();
            while (!candidates.isEmpty() && alphaSet.size() < kadA) {
                Node n = candidates.poll();
                if (!contacted.contains(n)) {
                    alphaSet.add(n);
                    contacted.add(n);
                }
            }

            if (alphaSet.isEmpty()) break;
            hops++;

            long maxHopLatency = 0;
            for (Node node : alphaSet) {
                long hopLatency = calculateLatency(CommonState.getNode(), node);
                maxHopLatency = Math.max(maxHopLatency, hopLatency);
            }
            latency += maxHopLatency;

            for (Node node : alphaSet) {
                HKademliaProtocol peerProto = (HKademliaProtocol) node.getProtocol(pid);
                List<Node> neighbors = peerProto.findClosestPeers(contentId, kadK);
                for (Node neighbor : neighbors) {
                    if (!contacted.contains(neighbor)) {
                        candidates.add(neighbor);
                    }
                }
                for (Node n : neighbors) {
                    if (!closestNodes.contains(n)) {
                        closestNodes.add(n);
                        changed = true;
                    }
                }
                closestNodes.sort(Comparator.comparingLong(n -> xorDistance(n.getID(), contentId)));
                if (closestNodes.size() > kadK) {
                    closestNodes = closestNodes.subList(0, kadK);
                }
            }
        }

        for (Node node : closestNodes) {
            HKademliaProtocol proto = (HKademliaProtocol) node.getProtocol(pid);
            
            if(proto.getClusterId() == sourceClusterId) {
                localIntraMessages++;
            } else {
                localInterMessages++;
            }

            proto.localStore.add(contentId);
            receivers++;
        }

        receivers = Math.min(receivers, kadK);

        this.intraClusterStore += localIntraMessages;
        this.interClusterStore += localInterMessages;
        return new HKademliaStoreLookupSimulator.StoreResult(hops, latency, receivers, localIntraMessages, localInterMessages);
    }

    public HKademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        int lookupInterMessages = 0;
        int lookupIntraMessages = 0;
        if (localStore.contains(contentId)) {
            return new HKademliaStoreLookupSimulator.LookupResult(true, 0, 0, 0, 1);
        }

        Set<Node> contacted = new HashSet<>();
        PriorityQueue<Node> shortestDistances = new PriorityQueue<>(Comparator.comparingLong(n -> xorDistance(n.getID(), contentId)));
        shortestDistances.addAll(findClosestPeers(contentId, kadA));

        int hops = 0;
        long latency = 0;
        boolean success = false;
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        

        int sourceClusterId = this.getClusterId();

        while (!shortestDistances.isEmpty()) {
            List<Node> newPeers = new ArrayList<>(kadA);
            Iterator<Node> iterator = shortestDistances.iterator();
            while (iterator.hasNext() && newPeers.size() < kadA) {
                Node n = iterator.next();
                if (!contacted.contains(n)) {
                    newPeers.add(n);
                }
            }
            if (newPeers.isEmpty()) break;

            for (Node peer : newPeers) {
                contacted.add(peer);
                hops++;
                latency++;

                HKademliaProtocol peerProtocol = (HKademliaProtocol) peer.getProtocol(pid);
                if (peerProtocol.getClusterId() == sourceClusterId) {
                    lookupIntraMessages++;
                } else {
                    lookupInterMessages++;
                }
                if (peerProtocol.localStore.contains(contentId)) {
                    success = true;
                    break;
                }
                shortestDistances.addAll(peerProtocol.findClosestPeers(contentId, kadK));
            }
            if (success) break;
        }

        this.intraClusterLookup += lookupIntraMessages;
        this.interClusterLookup += lookupInterMessages;

        return new HKademliaStoreLookupSimulator.LookupResult(success, hops, latency, lookupIntraMessages, lookupInterMessages);
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

    private long calculateLatency(Node from, Node to) {
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int fromCluster = ((HKademliaProtocol) from.getProtocol(pid)).getClusterId();
        int toCluster = ((HKademliaProtocol) to.getProtocol(pid)).getClusterId();
        long intraClusterLatency = 5 + (long) (Math.random() * 5);
        long interClusterLatency = 20 + (long) (Math.random() * 20);
        return (fromCluster == toCluster) ? intraClusterLatency : interClusterLatency;
    }

    public void registerContentOrigin(String contentId, int clusterId) {
        contentOriginCluster.put(contentId, clusterId);
    }

    public Integer getContentOriginCluster(String contentId) {
        return contentOriginCluster.get(contentId);
    }

    public int getKadK() {
        // Return the configured k-bucket size
        return this.kadK;
    }

    public int getKBucketSize() {
        return kbucket.size();
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
