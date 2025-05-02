import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;
import peersim.util.*;
import java.util.*;
import java.io.*;

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
    
    // Per-tick metrics
    private int tickStoreRequests = 0;
    private int tickStoreHops = 0;
    private long tickStoreLatency = 0;
    private int tickStoreReceivers = 0;

    // inter:intra

    private int tickStoreInter = 0;
    private int tickStoreIntra = 0;
    private int tickLookupIntra = 0;
    private int tickLookupInter = 0;


    private final List<Long> storedKeys = new ArrayList<>();
    private final Map<Long, Integer> contentReceivers = new HashMap<>();

    private final int tickSize = 15000;
    private final int totalRequests = 150000;

    public HKademliaStoreLookupSimulator(String prefix) {
        this.protocolID = Configuration.getPid(prefix + ".protocol");
        this.kadK = Configuration.getInt(prefix + ".kadK", 20); // Default to IPFS standard
        this.kadA = Configuration.getInt(prefix + ".kadA", 3);  // Default to IPFS standard
        this.operationType = Configuration.getString(prefix + ".type", "storelookup");
    }

    @Override
    public boolean execute() {
        Random rand = new Random(CommonState.r.nextLong());
        List<Double> storeHopsPerTick = new ArrayList<>();
        List<Double> storeLatencyPerTick = new ArrayList<>();
        List<Integer> storeReceiversPerTick = new ArrayList<>();
        List<Integer> bucketSizePerTick = new ArrayList<>();
        List<Double> storeInterIntraPerTick = new ArrayList<>();
        List<Double> lookupInterIntraPerTick = new ArrayList<>();
        int totalKBucketSize = 0;

        for (int i = 0; i < totalRequests; i++) {
            int initiatorID = rand.nextInt(Network.size());
            Node initiatorNode = Network.get(initiatorID);
            HKademliaProtocol protocol = (HKademliaProtocol) initiatorNode.getProtocol(protocolID);


            long baseId = initiatorNode.getID();
            String contentID = generateKeyNearNode(baseId, 8);
            long contentKey = Long.parseLong(contentID, 16);

            int bucketSize = protocol.getKBucketSize();
            // System.out.println("Bucket size: " + bucketSize);
            totalKBucketSize += bucketSize;

            // Store operation
            if (operationType.equals("storelookup") || operationType.equals("store")) {
                StoreResult storeResult = protocol.executeStore(contentKey);
                
                // Update receiver count for this content
                int currentReceivers = contentReceivers.getOrDefault(contentKey, 0);
                contentReceivers.put(contentKey, currentReceivers + storeResult.actualReceivers);
                
                storedKeys.add(contentKey);
                totalStoreRequests++;
                tickStoreRequests++;
                tickStoreHops += storeResult.hops;
                tickStoreLatency += storeResult.latency;
                tickStoreReceivers += storeResult.actualReceivers;
                tickStoreInter += storeResult.localInterMessages;
                tickStoreIntra += storeResult.localIntraMessages;
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
                        tickLookupIntra += result.lookupIntraMessages;
                        tickLookupInter += result.lookupInterMessages;
                    }
                }
            }

            // Collect metrics at each tick interval
            if (i % tickSize == 0 && i > 0) {
                double avgStoreHops = tickStoreRequests > 0 ? 
                    (double) tickStoreHops / tickStoreRequests : 0;
                double avgStoreLatency = tickStoreRequests > 0 ? 
                    (double) tickStoreLatency / tickStoreRequests : 0;
                double avgReceivers = tickStoreRequests > 0 ? 
                    (double) tickStoreReceivers / tickStoreRequests : 0;

                storeHopsPerTick.add(avgStoreHops);
                storeLatencyPerTick.add(avgStoreLatency);
                storeReceiversPerTick.add((int) Math.round(avgReceivers));
                bucketSizePerTick.add(totalKBucketSize);
                storeInterIntraPerTick.add((double)tickStoreInter/tickStoreIntra);
                lookupInterIntraPerTick.add((double)tickLookupInter/tickLookupIntra);


                // Reset tick counters
                tickStoreRequests = 0;
                tickStoreHops = 0;
                tickStoreLatency = 0;
                tickStoreReceivers = 0;

                System.out.println(totalKBucketSize);
                System.out.println((double)tickStoreInter/tickStoreIntra);
                System.out.printf("Store Inter/Intra: %d/%d%n", tickStoreInter, tickStoreIntra);
                System.out.println((double)tickLookupInter/tickLookupIntra);
                System.out.printf("Lookup Inter/Intra: %d/%d%n", tickLookupInter, tickLookupIntra);
                
            }
        }

        // Write metrics to CSV
        writeMetricsToCSV(storeHopsPerTick, storeLatencyPerTick, storeReceiversPerTick);
        writeMetricToCSV(bucketSizePerTick, storeInterIntraPerTick, lookupInterIntraPerTick);
        
        // Print summary
        printSummary(storeHopsPerTick, storeLatencyPerTick, storeReceiversPerTick);

        System.out.println(totalKBucketSize);
        System.out.println((double)tickStoreInter/tickStoreIntra);
        System.out.printf("Store Inter/Intra: %d/%d%n", tickStoreInter, tickStoreIntra);
        System.out.println((double)tickLookupInter/tickLookupIntra);
        System.out.printf("Lookup Inter/Intra: %d/%d%n", tickLookupInter, tickLookupIntra);
        
        return false;
    }

    private void writeMetricsToCSV(List<Double> hops, List<Double> latency, List<Integer> receivers) {
        String filename = "store_metrics_hkademlia_with_caching.csv";
        try (PrintWriter writer = new PrintWriter(filename)) {
            writer.println("Tick,AvgStoreHops,AvgStoreLatency(ms),StoreReceivers");
            for (int i = 0; i < hops.size(); i++) {
                writer.printf("%d,%.2f,%.2f,%d%n",
                    i+1, hops.get(i), latency.get(i), receivers.get(i));
            }
        } catch (FileNotFoundException e) {
            System.err.println("Failed to write CSV: " + e.getMessage());
        }
    }

    private void writeMetricToCSV(List<Integer> hops, List<Double> latency, List<Double> receivers) {
        String filename = "cluster_metrics_hkademlia_with_caching.csv";
        try (PrintWriter writer = new PrintWriter(filename)) {
            writer.println("Tick,KBucket Size,InterToIntraCluster Ratio - Store,InterToIntraCluster Ratio - Lookup");
            for (int i = 0; i < hops.size(); i++) {
                writer.printf("%d,%d,%.4f,%f%n",
                    i+1, hops.get(i), latency.get(i), receivers.get(i));
            }
        } catch (FileNotFoundException e) {
            System.err.println("Failed to write CSV: " + e.getMessage());
        }
    }

    private void printSummary(List<Double> hops, List<Double> latency, List<Integer> receivers) {
        System.out.println("=== H-Kademlia Simulation Summary ===");
        System.out.println("\n=== H-Kademlia Store Operation Tick Metrics ===");
        System.out.println("Tick\tAvgStoreHops\tAvgStoreLatency(ms)\tStoreReceivers");
        
        for (int i = 0; i < hops.size(); i++) {
            System.out.printf("%d\t%.2f\t%.2f\t%d%n",
                i+1, hops.get(i), latency.get(i), receivers.get(i));
        }
        
        System.out.println("\nFinal Statistics:");
        System.out.println("Total STORE requests: " + totalStoreRequests);
        System.out.println("Total LOOKUP requests: " + totalLookupRequests);
        System.out.println("Successful LOOKUPs: " + successfulLookups);
        System.out.println("Average LOOKUP hops: " + 
            (successfulLookups > 0 ? (double)totalLookupHops/successfulLookups : "N/A"));
        System.out.println("Average LOOKUP latency (ms): " +
            (successfulLookups > 0 ? (double)totalLatency/successfulLookups : "N/A"));
    }

    private String generateKeyNearNode(long nodeId, int proximityBits) {
        Random rand = new Random();
        long mask = (1L << proximityBits) - 1;
        long offset = rand.nextLong() & mask;
        return String.format("%08x", (nodeId ^ offset) & 0xffffffffL);
    }

    public static class LookupResult {
        public final boolean success;
        public final int hops;
        public final long latency;
        public final int lookupInterMessages;
        public final int lookupIntraMessages;

        public LookupResult(boolean success, int hops, long latency, int lookupInterMessages, int lookupIntraMessages) {
            this.success = success;
            this.hops = hops;
            this.latency = latency;
            this.lookupInterMessages = lookupInterMessages;
            this.lookupIntraMessages = lookupIntraMessages;
        }
    }

    public static class StoreResult {
        public final int hops;
        public final long latency;
        public final int actualReceivers;
        public final int localIntraMessages;
        public final int localInterMessages;


        public StoreResult(int hops, long latency, int actualReceivers, int localIntraMessages, int localInterMessages) {
            this.hops = hops;
            this.latency = latency;
            this.actualReceivers = actualReceivers;
            this.localIntraMessages = localIntraMessages;
            this.localInterMessages = localInterMessages;
        }
    }
}