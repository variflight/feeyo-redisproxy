package com.feeyo.redis.engine.manage.stat;

import com.feeyo.util.topn.Counter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * lfu
 */
public class LFUCache {

    Map<String, Integer> keyValueMap;//cache K and V
    Map<String, Integer> keyCountersMap;//K and counters
    Map<Integer, Set<String>> counterListMap;//Counter and item list
    volatile  int capacity;
    volatile int min = -1;

    public LFUCache(int capacity) {
        this.capacity = capacity;
        this.keyValueMap = new ConcurrentHashMap<>();
        this.keyCountersMap = new ConcurrentHashMap<>();
        this.counterListMap = new ConcurrentHashMap<>();
        this.counterListMap.put(1, new LinkedHashSet<String>());
    }

    public int get(String key) {
        if (!keyValueMap.containsKey(key)) {
            return -1;
        }
        // Get the count from counts map
        int count = keyCountersMap.get(key);
        keyCountersMap.put(key, count + 1);  // increase the counter
        //
        // remove the element from the counter to linkedhashset
		Set<String> set1 = counterListMap.get(count);
		if (set1 != null) {
			set1.remove(key);
			//
			// when current min does not have any data, next one would be the min
	        if (set1.size() == 0) {
	            if (count > 1) {
	                counterListMap.remove(count);
	            }
	            if (count == min) {
	                min++;
	            }
	        }
		}
		//
		Set<String> set2 = counterListMap.get(count + 1);
        if (set2 == null) {
        	set2 = new LinkedHashSet<String>();
            counterListMap.put(count + 1, set2);
        }
        set2.add(key);
        return keyValueMap.get(key);
    }

    public synchronized void set(String key, int value) {
        if (capacity <= 0) {
            return;
        }
        // If key does exist, we are returning from here
        if (keyValueMap.containsKey(key)) {
            keyValueMap.put(key, value);
            get(key);
            return;
        }
        if (keyValueMap.size() >= capacity) {
        	Set<String> set1 = counterListMap.get(min);
        	if (set1 != null) {
	            String evit = set1.iterator().next();
	            set1.remove(evit);
	            keyValueMap.remove(evit);
	            keyCountersMap.remove(evit);
	            
        	}
        }
        //
        // If the key is new, insert the value and current min should be 1 of course
        keyValueMap.put(key, value);
        keyCountersMap.put(key, 1);
        min = 1;
        //
        Set<String> set2 = counterListMap.get(1);
        if (set2 != null) {
        	set2.add(key);
        }
    }

    public  Map<String, Integer> getCounts(){
        return keyCountersMap;
    }
    
    @SuppressWarnings("unchecked")
	public List<Counter<String>> getTop(){
         LinkedList<Counter<String>> counterList =new LinkedList<>();
        for (Map.Entry<String,Integer> entry:keyCountersMap.entrySet()){
            counterList.add(new Counter<String>(entry.getKey(),entry.getValue()));
        }
        Collections.sort(counterList, DESC_COMPARATOR);
        return counterList;
    }

    @SuppressWarnings("rawtypes")
    private static final Comparator DESC_COMPARATOR = new Comparator<Counter>() {
        @Override
        public int compare(Counter o1, Counter o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            }
            return o1.getCount() > o2.getCount() ? -1 : o1.getCount() == o2.getCount() ? 0 : 1;
        }

    };

    public void  clear(){
        keyValueMap.clear();
        keyCountersMap.clear();
        counterListMap.clear();
    }

}
