// Core Logic of HKademlia, how peers interact: routing,  KBucket updates, remote vs local peer logic
import peersim.core.*;
import peersim.config.*;

import java.io.ObjectInputFilter.Config;
import java.util.*;

public class HKademliaProtocol implements Protocol {
    private final int kadK;
    private final int kadA;
    private int clusterID;
    private Set<Node> kbucket;

    private int cacheSize;
    private int cacheHits = 0;
    private int cacheMisses = 0;

    private static final int DEFAULT_CACHE_SIZE = 500;
    private static final String PAR_CACHE_SIZE = "cache_size";

    private LinkedHashMap<String, Object> contentCache;


    public HKademliaProtocol(String prefix) {
        this.kadK = Config.getInt(prefix + ".kadK", 2);
        this.kadA = Config.getInt(prefix + ".kadA", 1);
        this.kbucket = new HashSet<>();

        cacheSize = Config.getInt(prefix + "." + PAR_CACHE_SIZE, DEFAULT_CACHE_SIZE);

        // FIFO strategy
        contentCache = new LinkedHashMap<String, Object>(cacheSize, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                return size() > cacheSize;
            }
        };

        //LRU strategy
        // contentCache = new LinkedHashMap<String, Object>(cacheSize, 0.75f, true) {
        //     @Override
        //     protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
        //         return size() > cacheSize;
        //     }
        // };

        // LFU strategy
        // contentCache = new LinkedHashMap<String, Object>(cacheSize, 0.75f, false) {
        //     @Override
        //     protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
        //         if (size() > cacheSize) {
        //             String leastFrequentKey = null;
        //             int leastFrequency = Integer.MAX_VALUE;
        //             for (Map.Entry<String, Object> entry : contentCache.entrySet()) {
        //                 int frequency = getFrequency(entry.getKey());
        //                 if (frequency < leastFrequency) {
        //                     leastFrequency = frequency;
        //                     leastFrequentKey = entry.getKey();
        //                 }
        //             }
        //             if (leastFrequentKey != null) {
        //                 contentCache.remove(leastFrequentKey);
        //             }
        //         }
        //         return false;
        //     }
        //     private int getFrequency(String key) {
        //         // Implement a method to get the frequency of the key
        //         // This could be a separate data structure that tracks access counts
        //         return 0; // Placeholder
        //     }
        // };
        
    }

    // The clone() method ensures that each peer gets a new instance of your protocol class
    public Object Clone(){
        return new HKademliaProtocol(Config.getString("protocol"));
    }

    public void addPeer(Node peer) {
        // Apply H-Kademlia KBucket insertion rules
    }

    public void removePeer(Node peer) {
        kbucket.remove(peer);
    }

    public void executeStore(long contentId) {
        // Simulate STORE action based on contentId and protocol
    }

    public void executeLookup(long contentId) {
        // Simulate LOOKUP action based on contentId
    }

    public void setClusterId(int id) {
        this.clusterId = id;
    }

    public int getClusterId() {
        return clusterId;
    }

    // store content in cache
    public void storeInCache(String contentId, Object content){
        contentCache.put(contentId, content);
    }

    // Search for content in local cache
    public Object searchCache(String contentId) {
        Object result = contentCache.get(contentId);
        
        // Update stats (optional)
        if (result != null) {
            cacheHits++;
        } else {
            cacheMisses++;
        }
        
        return result;
    }

    //Check if content exists in cache
    public boolean isCached(String contentId) {
        return contentCache.containsKey(contentId);
    }

    /**
     * Clear the entire cache
     */
    public void clearCache() {
        contentCache.clear();
    }

    /**
     * Get cache statistics
     * @return String representation of cache stats
     */
    public String getCacheStats() {
        int totalRequests = cacheHits + cacheMisses;
        double hitRatio = totalRequests > 0 ? (double)cacheHits / totalRequests : 0;
        
        return String.format("Cache size: %d/%d, Hits: %d, Misses: %d, Hit ratio: %.2f%%", 
                contentCache.size(), cacheSize, cacheHits, cacheMisses, hitRatio * 100);
    }



}
