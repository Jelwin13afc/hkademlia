import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;
import peersim.cdsim.*;
import peersim.util.*;

import java.util.*;
import java.io.PrintWriter;
import java.io.FileNotFoundException;


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
    private int tickStoreRequests = 0;
    private int tickStoreHops = 0;
    private long tickStoreLatency = 0;

    private final List<Long> storedKeys = new ArrayList<>();

    private final int tickSize = 15000;        
    private final int totalRequests = 150000;  
    private int requestCounter = 0;

    public HKademliaStoreLookupSimulator(String prefix) {
        this.protocolID = Configuration.getPid(prefix + ".protocol");
        this.kadK = Configuration.getInt(prefix + ".kadK", 2); // default 2
        this.kadA = Configuration.getInt(prefix + ".kadA", 1); // default 1
        this.operationType = Configuration.getString(prefix + ".type", "storelookup"); // storelookup or just lookup
    }

    @Override
    public boolean execute() {
        Random rand = new Random();
        Set<Long> currentTickReceivers = new HashSet<>();
        List<Double> storeHopsPerTick = new ArrayList<>();
        List<Double> storeLatencyPerTick = new ArrayList<>();
        List<Integer> storeReceiversPerTick = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            int initiatorID = rand.nextInt(Network.size());
            Node initiatorNode = Network.get(initiatorID);
            HKademliaProtocol protocol = (HKademliaProtocol) initiatorNode.getProtocol(protocolID);

            long baseId = initiatorNode.getID();
            String contentID = generateKeyNearNode(baseId, 8);

            // Store operation
            if (operationType.equals("storelookup") || operationType.equals("store")) {
                StoreResult storeResult = protocol.executeStore(Long.parseLong(contentID, 16));
                storedKeys.add(Long.parseLong(contentID, 16));
                currentTickReceivers.add(initiatorNode.getID());
                totalStoreRequests++;
                tickStoreRequests++;
                tickStoreHops += storeResult.hops;
                tickStoreLatency += storeResult.latency;
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

            if (i % tickSize == 0) {
                double avgStoreHops = tickStoreRequests > 0 ? (double) tickStoreHops / tickStoreRequests : 0;
                double avgStoreLatency = tickStoreRequests > 0 ? (double) tickStoreLatency / tickStoreRequests : 0;

                int receivers = currentTickReceivers.size();

                storeHopsPerTick.add(avgStoreHops);
                storeLatencyPerTick.add(avgStoreLatency);
                storeReceiversPerTick.add(receivers);

                // Reset tick-specific metrics
                tickStoreRequests = 0;
                tickStoreHops = 0;
                tickStoreLatency = 0;
                currentTickReceivers.clear();
            }
        }

        String filename = "store_metrics.csv";
        try{
            PrintWriter writer = new PrintWriter(filename);
            writer.println("Tick,AvgStoreHops,AvgStoreLatency(ms),StoreReceivers");

            for (int i = 0; i < storeHopsPerTick.size(); i++) {
                writer.printf(
                    "%d,%.2f,%.2f,%d\n",
                    (i + 1),
                    storeHopsPerTick.get(i),
                    storeLatencyPerTick.get(i),
                    storeReceiversPerTick.get(i)
                );
            }
            writer.close();
        } catch (Exception e) {
            System.err.println("Failed to write CSV: " + e.getMessage());
        }
            

        // Print evaluation results
        System.out.println("=== H-Kademlia Simulation Summary ===");
        System.out.println("\n=== H-Kademlia Store Operation Tick Metrics ===");
        System.out.println("Tick\tAvgStoreHops\tAvgStoreLatency(ms)\tStoreReceivers");

        for (int i = 0; i < storeHopsPerTick.size(); i++) {
            System.out.printf(
                "%d\t%.2f\t%.2f\t%d\n",
                (i + 1),
                storeHopsPerTick.get(i),
                storeLatencyPerTick.get(i),
                storeReceiversPerTick.get(i)
            );
        }
        System.out.println("STORE requests: " + totalStoreRequests);
        System.out.println("LOOKUP requests: " + totalLookupRequests);
        System.out.println("Successful LOOKUPs: " + successfulLookups);
        System.out.println("Average LOOKUP hops: " +
            (successfulLookups > 0 ? (double) totalLookupHops / successfulLookups : "N/A"));
        System.out.println("Average LOOKUP latency (ms): " +
            (successfulLookups > 0 ? (double) totalLatency / successfulLookups : "N/A"));
        return false;
    }

    private String generateKeyNearNode(long nodeId, int proximityBits) {
        Random rand = new Random();
        long mask = (1L << proximityBits) - 1; 
        long offset = rand.nextLong() & mask;  
        long keyLong = nodeId ^ offset;        

        // Return 8-char lowercase hex string
        return String.format("%08x", keyLong & 0xffffffffL);
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

    public static class StoreResult {
        public int hops;
        public long latency;

        public StoreResult(int hops, long latency) {
            this.hops = hops;
            this.latency = latency;
        }
    }
}
