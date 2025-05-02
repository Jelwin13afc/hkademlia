import peersim.core.*;
import peersim.config.*;
import java.util.*;

public class KademliaProtocol implements Protocol {
    private final int kadK;
    private final int kadA;
    private final String prefix;
    public final Set<Node> kbucket;
    private final Set<Long> localStore;

    public KademliaProtocol(String prefix) {
        this.prefix = prefix;
        this.kadK = Configuration.getInt(prefix + ".kadK");
        this.kadA = Configuration.getInt(prefix + ".kadA");
        this.kbucket = new HashSet<>();
        this.localStore = new HashSet<>();
    }

    public Object clone() {
        return new KademliaProtocol(prefix);
    }

    public void addPeer(Node peer) {
        if (kbucket.size() < kadK * 2) { // Slightly larger bucket for standard Kademlia
            kbucket.add(peer);
        }
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public KademliaStoreLookupSimulator.StoreResult executeStore(long contentId) {
        localStore.add(contentId);
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);
        Node currentNode = CommonState.getNode();

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

            // Calculate max latency for this parallel hop
            long maxHopLatency = 0;
            for (Node node : alphaSet) {
                long hopLatency = calculateLatency(currentNode, node);
                maxHopLatency = Math.max(maxHopLatency, hopLatency);
            }
            latency += maxHopLatency;

            for (Node node : alphaSet) {
                KademliaProtocol peerProto = (KademliaProtocol) node.getProtocol(pid);
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

        // Store on closest nodes
        for (int i = 0; i < Math.min(kadK, closestNodes.size()); i++) {
            Node node = closestNodes.get(i);
            ((KademliaProtocol) node.getProtocol(pid)).localStore.add(contentId);
            receivers++;
        }

        return new KademliaStoreLookupSimulator.StoreResult(hops, latency, receivers);
    }

    private long calculateLatency(Node from, Node to) {
        // Virtual cluster simulation (same as lookup)
        int virtualClusters = 5;
        long fromCluster = from.getID() % virtualClusters;
        long toCluster = to.getID() % virtualClusters;

        // Same latency model as lookup
        long intraClusterLatency = 5 + (long)(Math.random() * 5);   // 5-10ms
        long interClusterLatency = 20 + (long)(Math.random() * 20); // 20-40ms

        return (fromCluster == toCluster) ? intraClusterLatency : interClusterLatency;
    }

    public KademliaStoreLookupSimulator.LookupResult executeLookup(long contentId) {
        if (localStore.contains(contentId)) {
            return new KademliaStoreLookupSimulator.LookupResult(true, 0, 0);
        }

        Set<Node> contacted = new HashSet<>();
        PriorityQueue<Node> shortestDistances = new PriorityQueue<>(
            Comparator.comparingLong(n -> xorDistance(n.getID(), contentId))
        );
        shortestDistances.addAll(findClosestPeers(contentId, kadA));

        int hops = 0;
        long latency = 0;
        boolean success = false;
        String protocolId = prefix.substring(prefix.lastIndexOf('.') + 1);
        int pid = Configuration.lookupPid(protocolId);

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
                latency += 10 + (long)(Math.random() * 10); // Same 10-20ms latency

                KademliaProtocol peerProtocol = (KademliaProtocol) peer.getProtocol(pid);
                if (peerProtocol.localStore.contains(contentId)) {
                    success = true;
                    break;
                }
                shortestDistances.addAll(peerProtocol.findClosestPeers(contentId, kadK));
            }
            if (success) break;
        }

        return new KademliaStoreLookupSimulator.LookupResult(success, hops, latency);
    }

    private List<Node> findClosestPeers(long targetId, int count) {
        PriorityQueue<Node> pq = new PriorityQueue<>(
            Comparator.comparingLong(n -> xorDistance(n.getID(), targetId))
        );
        pq.addAll(kbucket);
        List<Node> result = new ArrayList<>();
        while (!pq.isEmpty() && result.size() < count) {
            result.add(pq.poll());
        }
        return result;
    }

    private long xorDistance(long id1, long id2) {
        return id1 ^ id2;
    }
}