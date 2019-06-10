package com.feeyo.redis.engine.manage.stat;

import com.feeyo.util.topn.Counter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * lfu

 */
public class LFUCache {

    Map<String, Integer> vals;//cache K and V
    Map<String, Integer> counts;//K and counters
    Map<Integer, LinkedHashSet<String>> lists;//Counter and item list
    int cap;
    int min = -1;

    public LFUCache(int capacity) {
        cap = capacity;
        vals = new ConcurrentHashMap<>();
        counts = new ConcurrentHashMap<>();
        lists = new ConcurrentHashMap<>();
        lists.put(1, new LinkedHashSet<String>());
    }

    public int get(String key) {
        if (!vals.containsKey(key))
            return -1;
        // Get the count from counts map
        int count = counts.get(key);
        // increase the counter
        counts.put(key, count + 1);
        // remove the element from the counter to linkedhashset
        lists.get(count).remove(key);

        // when current min does not have any data, next one would be the min
        if (count == min && lists.get(count).size() == 0)
            min++;
        if (!lists.containsKey(count + 1))
            lists.put(count + 1, new LinkedHashSet<String>());
        lists.get(count + 1).add(key);
        return vals.get(key);
    }

    public void set(String key, int value) {
        if (cap <= 0)
            return;
        // If key does exist, we are returning from here
        if (vals.containsKey(key)) {
            vals.put(key, value);
            get(key);
            return;
        }
        if (vals.size() >= cap) {
            String evit = lists.get(min).iterator().next();
            lists.get(min).remove(evit);
            vals.remove(evit);
            counts.remove(evit);
        }
        // If the key is new, insert the value and current min should be 1 of course
        vals.put(key, value);
        counts.put(key, 1);
        min = 1;
        lists.get(1).add(key);
    }

    public  Map<String, Integer> getCounts(){
        return counts;
    }
    public List<Counter<String>> getTop(){
         LinkedList<Counter<String>> counterList =new LinkedList<>();
        for (Map.Entry<String,Integer> entry:counts.entrySet()){
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
        vals.clear();
        counts.clear();
        lists.clear();
    }

}
