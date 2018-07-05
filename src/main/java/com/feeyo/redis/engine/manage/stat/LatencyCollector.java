package com.feeyo.redis.engine.manage.stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LatencyCollector {
    private static Map<String, Map<Long, Queue<Long>>> nodeLatencyMap = new ConcurrentHashMap<>();
    // 定时任务定期进行缓存减少计算耗时
    private static Map<String, List<LatencyStatInfo>> nodeStatMap = new HashMap<>();
    // 倒叙排列
    private static Comparator<Long> epochComparator = new Comparator<Long>() {

        @Override
        public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
        }
    };

    /**
     * 按epoch逆序作统计并缓存
     */
    public static void doStatistics() {
        TreeMap<Long, Queue<Long>> sortedMap = new TreeMap<>(epochComparator);
        List<LatencyStatInfo> statInfoList;
        LatencyStatInfo tempObj;

        for (Map.Entry<String, Map<Long, Queue<Long>>> entry : nodeLatencyMap.entrySet()) {

            statInfoList = new ArrayList<>();
            sortedMap.clear();
            sortedMap.putAll(entry.getValue());

            for (Map.Entry<Long, Queue<Long>> tempEntry : sortedMap.entrySet()) {

                tempObj = new LatencyStatInfo(entry.getKey(), tempEntry.getKey());
                tempObj.calculate(tempEntry.getValue());
                statInfoList.add(tempObj);
            }
            nodeStatMap.put(entry.getKey(), statInfoList);
        }
    }

    public static void add(String nodeId, Long epoch, long cost) {
        Map<Long, Queue<Long>> epochMap = nodeLatencyMap.get(nodeId);

        if (epochMap == null) {
            // 只保存50个epoch的latency数据
            epochMap = Collections.synchronizedMap(new LRUMap<Long, Queue<Long>>(50));
            nodeLatencyMap.put(nodeId, epochMap);
        }

        Queue<Long> costQueue = epochMap.get(epoch);
        if (costQueue == null) {
            costQueue = new ConcurrentLinkedQueue<>();
            epochMap.put(epoch, costQueue);
        }

        costQueue.add(cost);
    }

    /**
     * 用于控制台监控展示, 返回指定节点的统计信息
     *
     * @return
     */
    public static List<String> getNodeLatencyResult(String nodeId) {
        List<String> resultList = new ArrayList<>();
        List<LatencyStatInfo> statInfoList = nodeStatMap.get(nodeId);

        for (LatencyStatInfo statInfo : statInfoList) {

            resultList.add(statInfo.toString());
        }
        return resultList;
    }

    /**
     * 判断节点是否负载
     *
     * @param nodeId    ip:port
     * @param threshold 平均延迟的阈值, 达到则认为节点过载
     * @return
     */
    public static boolean isOverLoad(String nodeId, double threshold) {
        List<LatencyStatInfo> statInfoList = nodeStatMap.get(nodeId);

        int i = 0;
        double tempEpoch = 0.0;
        // 最多取最近的10个epoch进行判断
        int size = statInfoList.size() >= 10 ? 10 : statInfoList.size();
        for (LatencyStatInfo statInfo : statInfoList) {

            if (i == size) break;

            tempEpoch += statInfo.getAvgLatency();
            i++;
        }

        if (threshold <= tempEpoch / size) {
            return true;
        }

        return false;
    }

    public static void reset() {
        nodeLatencyMap.clear();
        nodeStatMap.clear();
    }

    static class LRUMap<K, V> extends LinkedHashMap<K, V> {
        final int maxSize;

        public LRUMap(int maxSize) {
            super();
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxSize;
        }
    }

    static class LatencyStatInfo {
        private String nodeId;
        // Map的key是包装类因此不用primitive
        private Long epoch;
        private long minLatency;
        private long maxLatency;
        private double avgLatency;
        private long count;

        public LatencyStatInfo(String nodeId, Long epoch) {
            this.epoch = epoch;
            this.nodeId = nodeId;
        }

        public void calculate(Queue<Long> costQueue) {
            count = costQueue.size();
            long min, max, all;
            max = all = 0L;
            min = Integer.MAX_VALUE;

            Iterator<Long> itr = costQueue.iterator();
            Long temp;
            while (itr.hasNext()) {
                temp = itr.next();
                min = Math.min(min, temp);
                max = Math.max(max, temp);
                all += temp;
            }
            minLatency = min;
            maxLatency = max;
            avgLatency = all / count;
        }

        public double getAvgLatency() {
            return avgLatency;
        }

        public String toString() {
            return String.format("The node [%s] on epoch {%d} latency min: %d, max: %d, avg: %.2f (%d requests)", nodeId, epoch,
                    minLatency, maxLatency, avgLatency, count);
        }
    }
}