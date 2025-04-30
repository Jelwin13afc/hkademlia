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
        List<Double> avgKBucketSizes = new ArrayList<>();
        List<Double> storeRatios = new ArrayList<>();
        List<Double> lookupRatios = new ArrayList<>();

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

            // Log average KBucket size every tick
            if ((i + 1) % 10 == 0) {
                int tick = (i + 1) / 10;
                int totalPeers = Network.size();
                int totalKBucketSize = 0;
                int interStore = 0, intraStore = 0, interLookup = 0, intraLookup = 0;

                // Aggregate kbucket size and message stats
                for (int j = 0; j < totalPeers; j++) {
                    Node node = Network.get(j);
                    HKademliaProtocol proto = (HKademliaProtocol) node.getProtocol(protocolID);
                    totalKBucketSize += proto.getKBucketSize();

                    interStore += proto.getInterStoreMessages();
                    intraStore += proto.getIntraStoreMessages();
                    interLookup += proto.getInterLookupMessages();
                    intraLookup += proto.getIntraLookupMessages();

                }

                double avgKBucketSize = (double) totalKBucketSize / totalPeers;
                avgKBucketSizes.add(avgKBucketSize);
                System.out.printf("Tick %d,%f\n", tick, avgKBucketSize); // CSV format: Tick,AvgKBucketSize

                double storeRatio = intraStore > 0 ? (double) interStore / intraStore : 0;
                double lookupRatio = intraLookup > 0 ? (double) interLookup / intraLookup : 0;
                storeRatios.add(storeRatio);
                lookupRatios.add(lookupRatio);
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
        System.out.println("=== Average KBucket Sizes per Tick ===");
        for (int t = 0; t < avgKBucketSizes.size(); t++) {
            System.out.printf("Tick %d: %.2f\n", t + 1, avgKBucketSizes.get(t));
        }
        System.out.println("=== Store Ratios per Tick ===");
        for (int t = 0; t < storeRatios.size(); t++) {
            System.out.printf("Tick %d: %.2f\n", t + 1, storeRatios.get(t));
        }
        System.out.println("=== Lookup Ratios per Tick ===");
        for (int t = 0; t < lookupRatios.size(); t++) {
            System.out.printf("Tick %d: %.2f\n", t + 1, lookupRatios.get(t));
        }


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
