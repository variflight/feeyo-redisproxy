package com.feeyo.kafka.net.backend.broker.zk;

import java.text.MessageFormat;

/**
 * Offset 路径
 *
 */
public class ZkPathUtil {
	
	// 集群
	private static final String CLUSTER                	    				= "/{0}/cluster";										// 集群		
	private static final String CLUSTER_HOST								= "/{0}/cluster/{1}";									// 集群节点
	private static final String MASTER_RUNNING                     			= "/{0}/master/running";								// 服务端当前正在提供服务的running节点

	// 分区点位
	private static final String PARTITION									= "/{0}/partition/";
	private static final String PARTITION_LOGSTART_OFFSET					= "/{0}/partition/{1}/offset/logstart";					// logStartOffset
	private static final String PARTITION_PRODUCER_OFFSET              		= "/{0}/partition/{1}/offset/producer";		    		// 生产者点位
	
	private static final String PARTITION_CONSUMER              			= "/{0}/partition/{1}/offset/consumers";				// 消费者
	private static final String PARTITION_CONSUMER_OFFSET             		= "/{0}/partition/{1}/offset/consumers/{2}";			// 消费者点位
	private static final String PARTITION_CONSUMER_ROLLBACK_OFFSET         	= "/{0}/partition/{1}/offset/consumers/{2}/rollback";	// 消费不成功，回退的点位

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

    public String getPartitionPath() {
        return MessageFormat.format(PARTITION, rootPath);
    }
    
    public String getPartitionLogStartOffsetPath(int partition) {
        return MessageFormat.format(PARTITION_LOGSTART_OFFSET, rootPath, partition);
    }
    
    public String getPartitionProducerOffsetPath(int partition) {
        return MessageFormat.format(PARTITION_PRODUCER_OFFSET, rootPath, partition);
    }
    
    public String getPartitionConsumerPath(int partition) {
        return MessageFormat.format(PARTITION_CONSUMER, rootPath, partition);
    }
    
    public String getPartitionConsumerOffsetPath(int partition, String consumer) {
        return MessageFormat.format(PARTITION_CONSUMER_OFFSET, rootPath, partition, consumer);
    }
    
    public String getPartitionConsumerRollbackOffsetPath(int partition, String consumer) {
        return MessageFormat.format(PARTITION_CONSUMER_ROLLBACK_OFFSET, rootPath, partition, consumer);
    }
}
