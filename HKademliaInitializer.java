// Assigns each peer to a cluster, count is set to 5
// Initializes their K-Buckets
// Optionally assigns peer IDs, populates lookup indices, etc.

import peersim.core.*;
import peersim.config.*;
import peersim.edsim.*;

public class HKademliaInitializer implements Control {

    private final String protocol;

    public HKademliaInitializer(String prefix) {
        this.protocol = prefix + ".protocol";
    }

    public boolean execute() {
        int pid = Config.getPid(protocol);
        int numClusters = Config.getInt("hkademlia.clusters", 5);

        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            HKademliaProtocol prot = (HKademliaProtocol) node.getProtocol(pid);
            prot.setClusterId(i % numClusters);
            // Optionally, pre-fill KBuckets here
        }
        return false;
    }
}

