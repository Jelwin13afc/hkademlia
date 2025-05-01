// Assigns each peer to a cluster, count is set to 5
// Initializes their K-Buckets
// Optionally assigns peer IDs, populates lookup indices, etc.

import peersim.core.*;
import peersim.config.*;
import peersim.edsim.*;

public class HKademliaInitializer implements Control {

    private final String protocol;

    public HKademliaInitializer(String prefix) {
        this.protocol = Configuration.getString(prefix + ".protocol");
    }

    public boolean execute() {
        int pid = Configuration.lookupPid(protocol);
        int numClusters = Configuration.getInt("protocol." + protocol + ".clusters", 5);

        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            HKademliaProtocol prot = (HKademliaProtocol) node.getProtocol(pid);
            prot.setClusterId(i % numClusters);
        }
        // Fill KBuckets for each peer
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            HKademliaProtocol protocol = (HKademliaProtocol) node.getProtocol(pid);
            for (int j = 0; j < Network.size(); j++) {
                if (i != j) {
                    Node candidate = Network.get(j);
                    protocol.addPeer(node, candidate);
                }
            }
        }
        return false;
    }
}