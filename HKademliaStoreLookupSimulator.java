import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;
import peersim.cdsim.*;
import peersim.util.*;

import java.util.*;

public class HKademliaStoreLookupSimulator implements Control {

    private final int protocolID;
    private final int kadK;
    private final int kadA;
    private final String operationType;

    // Metrics
    private int totalStoreRequests = 0;
    private int totalLookupRequests = 0;
    private int successfulLookups = 0;
    private int totalLookupHops = 0;
    private long totalLatency = 0;
    private final List<Long> storedKeys = new ArrayList<>();

    public HKademliaStoreLookupSimulator(String prefix) {
        this.protocolID = Configuration.getPid(prefix + ".protocol");
        this.kadK = Configuration.getInt(prefix + ".kadK", 2); // default 2
        this.kadA = Configuration.getInt(prefix + ".kadA", 1); // default 1
        this.operationType = Configuration.getString(prefix + ".type", "storelookup"); // storelookup or just lookup
    }

    @Override
    public boolean execute() {
        int numRequests = 100; // You can make this configurable if needed
        Random rand = new Random();

        for (int i = 0; i < numRequests; i++) {
            int initiatorID = rand.nextInt(Network.size());
            Node initiatorNode = Network.get(initiatorID);
            HKademliaProtocol protocol = (HKademliaProtocol) initiatorNode.getProtocol(protocolID);

            String contentID = generateRandomKey();

            // Store operation
            if (operationType.equals("storelookup") || operationType.equals("store")) {
                protocol.executeStore(Long.parseLong(contentID, 16));
                storedKeys.add(Long.parseLong(contentID, 16));
                totalStoreRequests++;
            }

            // Lookup operation
            if (operationType.equals("storelookup") || operationType.equals("lookup")) {
                if (!storedKeys.isEmpty()) {
                    long key = storedKeys.get(rand.nextInt(storedKeys.size()));
                    LookupResult result = protocol.executeLookup(key);
                    totalLookupRequests++;
                    if (result.success) {
                        successfulLookups++;
                        totalLookupHops += result.hops;
                        totalLatency += result.latency;
                    }
                }
            }
        }

        // Print evaluation results
        System.out.println("=== H-Kademlia Simulation Summary ===");
        System.out.println("STORE requests: " + totalStoreRequests);
        System.out.println("LOOKUP requests: " + totalLookupRequests);
        System.out.println("Successful LOOKUPs: " + successfulLookups);
        System.out.println("Average LOOKUP hops: " +
            (successfulLookups > 0 ? (double) totalLookupHops / successfulLookups : "N/A"));
        System.out.println("Average LOOKUP latency (ms): " +
            (successfulLookups > 0 ? (double) totalLatency / successfulLookups : "N/A"));
        return false;
    }

    private String generateRandomKey() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    // Placeholder result structure; implement this in your protocol
    public static class LookupResult {
        public boolean success;
        public int hops;
        public long latency;

        public LookupResult(boolean success, int hops, long latency) {
            this.success = success;
            this.hops = hops;
            this.latency = latency;
        }
    }
}
