// Assigns each peer to a cluster (for metrics), and initializes their K-Buckets
import peersim.core.*;
import peersim.config.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KademliaInitializer implements Control {

    private final String protocol;
    private final int numClusters; // Configuration for number of clusters

    public KademliaInitializer(String prefix) {
        this.protocol = Configuration.getString(prefix + ".protocol");
        this.numClusters = Configuration.getInt(prefix + ".clusters", 5); // Default to 5 clusters
    }

    public boolean execute() {
        int pid = Configuration.lookupPid(protocol);

        // Assign each peer to a cluster (for metrics)
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            KademliaProtocol prot = (KademliaProtocol) node.getProtocol(pid);
            prot.setClusterId(i % numClusters);
        }

        // Fill KBuckets for each peer
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            KademliaProtocol protocol = (KademliaProtocol) node.getProtocol(pid);
            int kValue = protocol.getKadK();
            int targetSize = Math.max(1, kValue / 2);

            // Create a list of candidate nodes (all nodes except self)
            List<Node> candidates = new ArrayList<>();
            for (int j = 0; j < Network.size(); j++) {
                if (i != j) {
                    candidates.add(Network.get(j));
                }
            }

            // Shuffle the list to get a random selection
            Collections.shuffle(candidates, CommonState.r);

            int numToAdd = Math.min(targetSize, candidates.size());
            for (int j = 0; j < numToAdd; j++) {
                protocol.addPeer(node, candidates.get(j));
            }
        }

        return false;
    }
}