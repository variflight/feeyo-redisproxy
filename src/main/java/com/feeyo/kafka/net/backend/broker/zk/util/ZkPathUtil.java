package com.feeyo.kafka.net.backend.broker.zk.util;

import java.text.MessageFormat;

/**
 * Offset 路径
 *
 */
public class ZkPathUtil {
	
	// 集群
	private static final String CLUSTER                	    				= "{0}/cluster";										// 集群		
	private static final String CLUSTER_HOST								= "{0}/cluster/{1}";									// 集群节点
	private static final String MASTER_RUNNING                     			= "{0}/master/running";									// 服务端当前正在提供服务的running节点

	// 分区点位
	private static final String PARTITION									= "{0}/poolId/{1}/topic/{2}/partition";
	private static final String PARTITION_LOGSTART_OFFSET					= "{0}/poolId/{1}/topic/{2}/partition/{3}/offset/logstart";				 // logStartOffset
	private static final String PARTITION_PRODUCER_OFFSET              		= "{0}/poolId/{1}/topic/{2}/partition/{3}/offset/producer";		    	 // 生产者点位
	
	private static final String PARTITION_CONSUMER              			= "{0}/poolId/{1}/topic/{2}/partition/{3}/offset/consumers";				 // 消费者
	private static final String PARTITION_CONSUMER_OFFSET             		= "{0}/poolId/{1}/topic/{2}/partition/{3}/offset/consumers/{4}";			 // 消费者点位
	private static final String PARTITION_CONSUMER_RETURN_OFFSET    		= "{0}/poolId/{1}/topic/{2}/partition/{3}/offset/consumers/{4}/return";   	 // 消费不成功，回退的点位

	private String rootPath = null;
	
	public ZkPathUtil(String rootPath) {
		this.rootPath = rootPath;
	}
    
    public String getClusterPath() {
    	return MessageFormat.format(CLUSTER, rootPath);
    }
    
    public String getClusterHostPath(String address) {
    	return MessageFormat.format(CLUSTER_HOST, rootPath, address);
    }

    public String getMasterRunningPath() {
        return MessageFormat.format(MASTER_RUNNING, rootPath);
    }

    public String getPartitionPath(int poolId, String topic) {
        return MessageFormat.format(PARTITION, rootPath, poolId, topic);
    }
    
    public String getPartitionLogStartOffsetPath(int poolId, String topic, int partition) {
        return MessageFormat.format(PARTITION_LOGSTART_OFFSET, rootPath, poolId, topic, partition);
    }
    
    public String getPartitionProducerOffsetPath(int poolId, String topic, int partition) {
        return MessageFormat.format(PARTITION_PRODUCER_OFFSET, rootPath, poolId, topic, partition);
    }
    
    public String getPartitionConsumerPath(int poolId, String topic, int partition) {
        return MessageFormat.format(PARTITION_CONSUMER, rootPath, poolId, topic, partition);
    }
    
    public String getPartitionConsumerOffsetPath(int poolId, String topic, int partition, String consumer) {
        return MessageFormat.format(PARTITION_CONSUMER_OFFSET, rootPath, poolId, topic, partition, consumer);
    }
    
    public String getPartitionConsumerReturnOffsetPath(int poolId, String topic, int partition, String consumer) {
        return MessageFormat.format(PARTITION_CONSUMER_RETURN_OFFSET, rootPath, poolId, topic, partition, consumer);
    }
}
