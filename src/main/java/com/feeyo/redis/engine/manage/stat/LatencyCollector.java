package com.feeyo.redis.engine.manage.stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LatencyCollector {
    private static Map<String, Deque<Long>> nodeLatencyMap = new ConcurrentHashMap<>();
    private static final int MAX_QUEUE_SIZE = 50;

    public static void add(String nodeId, Long cost) {

        Deque<Long> costDeque = nodeLatencyMap.get(nodeId);
        if (costDeque == null) {
            costDeque = new ConcurrentLinkedDeque<>();
            nodeLatencyMap.put(nodeId, costDeque);
        }

        // 保持队列容量
        int size = costDeque.size();
        if (size >= MAX_QUEUE_SIZE) {
            costDeque.peekLast();
        }
        // 逆序排列
        costDeque.addFirst(cost);
    }

    public static boolean isOverLoad(String nodeId, int threshold) {
        Deque<Long> costDeque = nodeLatencyMap.get(nodeId);
        if (costDeque == null) {
            return false;
        }

        int queueSize = nodeLatencyMap.get(nodeId).size();
        int i = 0;
        long allLatency = 0L;
        int size = Math.max(10, queueSize);
        Iterator<Long> itr = costDeque.iterator();
        while (itr.hasNext()) {

            if (i == size) {
                break;
            }
            allLatency += itr.next();
            i++;
        }
        return allLatency / size >= threshold;
    }

    /**
     * 用于控制台监控展示, 返回指定节点的统计信息
     *
     * @return
     */
    public static List<String> getNodeLatencyResult(String nodeId) {

        List<String> result = new ArrayList<>();
        Deque<Long> costDeque = nodeLatencyMap.get(nodeId);
        if (costDeque == null) {
            return result;
        }

        int queueSize = nodeLatencyMap.get(nodeId).size();
        int i = 0;
        int size = Math.min(10, queueSize);
        Iterator<Long> itr = costDeque.iterator();
        StringBuffer latencyInfo = new StringBuffer("The node [");
        latencyInfo.append(nodeId).append("] recently latency is: ");
        while (itr.hasNext()) {

            if (i == size) {
                break;
            }
            latencyInfo.append(itr.next()).append(", ");
            i++;
        }
        result.add(latencyInfo.toString());
        return result;
    }
}