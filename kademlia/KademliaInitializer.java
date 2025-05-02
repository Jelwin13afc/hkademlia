import peersim.core.*;
import peersim.config.*;

public class KademliaInitializer implements Control {
    private final String protocol;

    public KademliaInitializer(String prefix) {
        this.protocol = Configuration.getString(prefix + ".protocol");
    }

    public boolean execute() {
        int pid = Configuration.lookupPid(protocol);

        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            KademliaProtocol protocol = (KademliaProtocol) node.getProtocol(pid);
            for (int j = 0; j < Network.size(); j++) {
                if (i != j) {
                    Node peer = Network.get(j);
                    protocol.addPeer(peer);
                }
            }
        }
        return false;
    }
}
