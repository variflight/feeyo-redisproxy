package com.feeyo.redis.engine.manage.stat;

import javafx.util.Pair;

import java.util.*;

/**
 * lfu
 * @param <K>
 * @param <V>
 */
public class LFUCache<K,V> {

    // a hash map holding <key, <frequency, value>> pairs
    private final Map<K, Pair<Integer, V>> cache;

    // a list of LinkedHashSet, freqList[i] has elements with frequency = i
    private final LinkedHashSet<K>[] freqList;

    // the minimum frequency in the cache
    private int minFreq;

    // the size of the cache; it is also the upper bound of possible frequency
    private final int capacity;

    // the number of evicted elements when reaching capacity
    private final int evict_num;

    /**
     * Create a new LFU cache.
     * @param  cap           the size of the cache
     * @param  evictFactor   the percentage of elements for replacement
     * @return a newly created LFU cache
     */
    @SuppressWarnings("unchecked")
    public LFUCache(int cap, double evictFactor) {
        if (cap <= 0 || evictFactor <= 0 || evictFactor >= 1) {
            throw new IllegalArgumentException("Eviction factor or Capacity is illegal.");
        }
        capacity = cap;
        minFreq = 0;  // the initial smallest frequency
        evict_num = Math.min(cap, (int)Math.ceil(cap * evictFactor));

        cache = new HashMap<K, Pair<Integer, V>>();
        freqList = new LinkedHashSet[cap];
        for (int i = 0; i < cap; ++i) {
            freqList[i] = new LinkedHashSet<K>();
        }
    }

    /**
     * Update frequency of the key-value pair in the cache if the key exists.
     * Increase the frequency of this pair and move it to the next frequency set.
     * If the frequency reaches the capacity, move it the end of current frequency set.
     * @return
     */
    private synchronized void touch(K key) {
        if (cache.containsKey(key)) { // sanity checking
            int freq = cache.get(key).getKey();
            V val = cache.get(key).getValue();
            // first remove it from current frequency set
            freqList[freq].remove(key);

            if (freq + 1 < capacity) {
                // update frequency
                cache.put(key, new Pair<>(freq + 1, val));
                freqList[freq + 1].add(key);
                if (freq == minFreq && freqList[minFreq].isEmpty()) {
                    // update current minimum frequency
                    ++minFreq;
                }
            }
            else {
                // LRU: put the most recent visited to the end of set
                freqList[freq].add(key);
            }
        }
    }

    /**
     * Evict the least frequent elements in the cache
     * The number of evicted elements is configured by eviction factor
     * @return
     */
    private synchronized void evict() {
        for (int i = 0; i < evict_num && minFreq < capacity; ++i) {
            // get the first element in the current minimum frequency set
            K key = (K) freqList[minFreq].iterator().next();
            freqList[minFreq].remove(key);
            cache.remove(key);
            while (minFreq < capacity && freqList[minFreq].isEmpty()) {
                // skip empty frequency sets
                ++minFreq;
            }
        }
    }

    /**
     * Get the value of key.
     * If the key does not exist, return null.
     * @param  key   the key to query
     * @return the value of the key
     */
    public synchronized V get(K key) {
        if (!cache.containsKey(key)) {
            return null;
        }
        // update frequency
        touch(key);
        return cache.get(key).getValue();
    }

    /**
     * Set key to hold the value.
     * If key already holds a value, it is overwritten.
     * @param  key   the key of the pair
     * @param  value the value of the pair
     * @return
     */
    public synchronized void set(K key, V value) {
        if (cache.containsKey(key)) {
            Integer freq = cache.get(key).getKey();
            cache.put(key, new Pair<>(freq, value));  // update value
            touch(key);  // update frequency
            return ;
        }
        if (cache.size() >= capacity) {
            evict();
        }
        // set the minimum frequency back to 0
        minFreq = 0;
        cache.put(key, new Pair<>(minFreq, value));
        freqList[minFreq].add(key);
    }

    /**
     * Returns the values of all specified keys.
     * For every key that does not exist, null is returned.
     * @param  pairs a list of keys to query
     * @return query results, a list of key-value pairs
     */
    public synchronized List<Pair<K, V>> mget(List<K> keys) {
        List<Pair<K, V>> ret = new ArrayList<Pair<K, V>>();
        for (K key : keys) {
            ret.add(new Pair<>(key, get(key)));
        }
        return ret;
    }

    /**
     * Sets the given keys to their respective values.
     * MSET replaces existing values with new values, just as regular SET.
     * @param  pairs a list of key-value pairs to be set
     * @return
     */
    public synchronized void mset(List<Pair<K, V>> pairs) {
        for (Pair<K, V> pair : pairs) {
            set(pair.getKey(), pair.getValue());
        }
    }

    /**
     * Increments the value stored at key by delta.
     * If the key does not exist, it is set to 0 before performing the operation.
     * Only works for integer value.
     * This function will increase frequency by 2
     * @param  key   the key needed to be increased
     * @param  delta increment
     * @return the value after increment
     */
    @SuppressWarnings("unchecked")
    public synchronized Integer incr(K key, Integer delta) {
        Integer value = (Integer) get(key);
        if (value == null) {
            value = 0;
        }
        value += delta;
        set(key, (V) value);
        return value;
    }

    /**
     * Decrements the value stored at key by delta.
     * If the key does not exist, it is set to 0 before performing the operation.
     * Only works for integer value.
     * This function will increase frequency by 2
     * @param  key   the key needed to be decreased
     * @param  delta decrement
     * @return the value after decrement
     */
    public synchronized Integer decr(K key, Integer delta) {
        return incr(key, -delta);
    }

    public synchronized void clear(){
        cache.clear();
    }

    public Map<K,V> getTop100(){
        Map<K,V> map= new HashMap<>();
        int count=0;
        for (int i = capacity-1; i > 0; i--) {
            LinkedHashSet<K> tempSet= freqList[i];
            if (!tempSet.isEmpty()){
                Iterator<K>  iterator=tempSet.iterator();
                while (iterator.hasNext()){
                    K key=iterator.next();
                    V value=cache.get(key).getValue();
                    map.put(key,value);
                    count=count++;
                    if (count>99){
                        return map;
                    }
                }
            }
        }
        return map;
    }
}
