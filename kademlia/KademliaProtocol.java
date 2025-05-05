import peersim.core.*;
import peersim.config.*;
import java.util.*;

public class KademliaProtocol implements Protocol {
    private final int kadK;
    private final int kadA;
    private Set<Node> kbucket;

    private final String prefix;

    private final Set<Long> localStore = new HashSet<>();

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
        this.contentOriginCluster = new HashMap<>();
    }

    public Object clone(){
        return new KademliaProtocol(prefix);
    }

    public void addPeer(Node selfNode, Node peer) {
        long selfId = selfNode.getID();
        long peerId = peer.getID();
        kbucket.add(peer);

        if (kbucket.size() > kadK) {
            List<Node> sortedBucket = new ArrayList<>(kbucket);
            sortedBucket.sort(Comparator.comparingLong(n -> xorDistance(n.getID(), selfId)));
            kbucket = new HashSet<>(sortedBucket.subList(0, kadK));
        }
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public KademliaStoreLookupSimulator.StoreResult executeStore(long contentId) {
        localStore.add(contentId);

        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int sourceClusterId = this.getClusterId();

        Set<Node> contacted = new HashSet<>();
        List<Node> closestNodes = findClosestPeers(contentId, kadK);
        PriorityQueue<Node> candidates = new PriorityQueue<>(
                Comparator.comparingLong(n -> xorDistance(n.getID(), contentId))
        );
        candidates.addAll(closestNodes);

        int hops = 0;
        long latency = 0;
        boolean changed = true;
        int receivers = 0;
        int localIntraMessages = 0;
        int localInterMessages = 0;

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
                long hopLatency = calculateLatency(getSelfNode(pid), node);
                maxHopLatency = Math.max(maxHopLatency, hopLatency);
            }
            latency += maxHopLatency;

            for (Node node : alphaSet) {
                KademliaProtocol peerProto = (KademliaProtocol) node.getProtocol(pid);
                Node selfNode = getSelfNode(pid);
                int peerClusterId = peerProto.getClusterId();

                if (peerClusterId == sourceClusterId) localIntraMessages++;
                else localInterMessages++;

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
                        this.addPeer(selfNode, n);
                    }
                }

                closestNodes.sort(Comparator.comparingLong(n -> xorDistance(n.getID(), contentId)));
                if (closestNodes.size() > kadK) {
                    closestNodes = closestNodes.subList(0, kadK);
                }
            }
        }

        for (Node node : closestNodes) {
            KademliaProtocol proto = (KademliaProtocol) node.getProtocol(pid);
            int peerClusterId = proto.getClusterId();

            if (peerClusterId == sourceClusterId) localIntraMessages++;
            else localInterMessages++;

            proto.localStore.add(contentId);
            receivers++;
        }

        receivers = Math.min(receivers, kadK);
        this.intraClusterStore += localIntraMessages;
        this.interClusterStore += localInterMessages;

        return new KademliaStoreLookupSimulator.StoreResult(hops, latency, receivers, localIntraMessages, localInterMessages);
    }

    public KademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        if (localStore.contains(contentId)) {
            return new KademliaStoreLookupSimulator.LookupResult(true, 0, 0, 0, 1);
        }

        Set<Node> contacted = new HashSet<>();
        PriorityQueue<Node> shortestDistances = new PriorityQueue<>(
                Comparator.comparingLong(n -> xorDistance(n.getID(), contentId))
        );
        shortestDistances.addAll(findClosestPeers(contentId, kadA));

        int hops = 0;
        long latency = 0;
        boolean success = false;
        String protocolId = prefix.substring(prefix.lastIndexOf('.')+1);
        int pid = Configuration.lookupPid(protocolId);
        int lookupInterMessages = 0;
        int lookupIntraMessages = 0;
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

                KademliaProtocol peerProtocol = (KademliaProtocol) peer.getProtocol(pid);
                int peerClusterId = peerProtocol.getClusterId();

                if (peerClusterId == sourceClusterId) lookupIntraMessages++;
                else lookupInterMessages++;

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

    private long calculateLatency(Node from, Node to) {
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        int fromCluster = ((KademliaProtocol)from.getProtocol(pid)).getClusterId();
        int toCluster = ((KademliaProtocol)to.getProtocol(pid)).getClusterId();

        long intraClusterLatency = 5 + (long)(Math.random() * 5);
        long interClusterLatency = 20 + (long)(Math.random() * 20);

        return (fromCluster == toCluster) ? intraClusterLatency : interClusterLatency;
    }

    public void registerContentOrigin(String contentId, int clusterId) {
        contentOriginCluster.put(contentId, clusterId);
    }

    public Integer getContentOriginCluster(String contentId) {
        return contentOriginCluster.get(contentId);
    }

    public int getKBucketSize() {
        return kbucket.size();
    }

    public int getKadK() {
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