// Assigns each peer to a cluster, count is set to 5
// Initializes their K-Buckets
// Optionally assigns peer IDs, populates lookup indices, etc.

// init 50% of bucket

import peersim.core.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
            int kValue = protocol.getKadK();
            int targetSize = Math.max(1, kValue / 2);

            // Create a list of candidate nodes (all nodes except self)
            List<Node> candidates = new ArrayList<>();
            for (int j = 0; j < Network.size(); j++) {
                if (i != j) {
                    candidates.add(Network.get(j));
                }
            }
            
            // Shuffle the list to get random selection
            Collections.shuffle(candidates, CommonState.r);

            int numToAdd = Math.min(targetSize, candidates.size());
            for (int j = 0; j < numToAdd; j++) {
                protocol.addPeer(node, candidates.get(j));
            }
        }



        return false;
    }
}
//     public boolean execute() {
//         int pid = Configuration.lookupPid(protocol);
//         int numClusters = Configuration.getInt("protocol." + protocol + ".clusters", 5);

//         for (int i = 0; i < Network.size(); i++) {
//             Node node = Network.get(i);
//             HKademliaProtocol prot = (HKademliaProtocol) node.getProtocol(pid);
//             prot.setClusterId(i % numClusters);
//         }
//         // Fill KBuckets for each peer
//         for (int i = 0; i < Network.size(); i++) {
//             Node node = Network.get(i);
//             HKademliaProtocol protocol = (HKademliaProtocol) node.getProtocol(pid);
//             for (int j = 0; j < Network.size(); j++) {
//                 if (i != j) {
//                     Node candidate = Network.get(j);
//                     protocol.addPeer(node, candidate);
//                 }
//             }
//         }
//         return false;
//     }
// }


