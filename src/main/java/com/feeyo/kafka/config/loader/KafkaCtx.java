package com.feeyo.kafka.config.loader;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.admin.KafkaAdmin;
import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.runtime.DataNode;
import com.feeyo.kafka.net.backend.runtime.DataPartition;
import com.feeyo.kafka.net.backend.runtime.Metadata;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

public class KafkaCtx {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaCtx.class );
	
	private final static KafkaCtx INSTANCE = new KafkaCtx();
	
	private ReentrantLock lock = new ReentrantLock();
	
	private KafkaCtx() {}

	public static KafkaCtx getInstance() {
		return INSTANCE;
	}
	

	public void load(Map<String, TopicCfg> topicCfgMap, PoolCfg poolCfg) {
		
		if (topicCfgMap == null || topicCfgMap.isEmpty()) {
			return;
		}
		
		// Get server address for kafka
		StringBuffer servers = new StringBuffer();
		List<String> nodes = poolCfg.getNodes();
		for (int i = 0; i < nodes.size(); i++) {
			String str = nodes.get(i);
			String[] node = str.split(":");
			servers.append(node[0]).append(":").append(node[1]);
			if (i < nodes.size() - 1) {
				servers.append(",");
			}
		}
		
		
		KafkaAdmin kafkaAdmin = null;
		try {
			// 获取 Kafka 管理对象
			kafkaAdmin = KafkaAdmin.create(servers.toString());
			
			// 获取kafka中的topic情况
			Map<String, TopicDescription> remoteTopics = kafkaAdmin.getTopicAndDescriptions();

			for (Entry<String, TopicCfg> entry : topicCfgMap.entrySet()) {

				TopicCfg topicCfg = entry.getValue();

				String topicName = topicCfg.getName();
				short replicationFactor = topicCfg.getReplicationFactor();
				int partitionNum = topicCfg.getPartitions();

				TopicDescription topicDescription = remoteTopics.get(topicName);
				if (topicDescription != null) {
					int oldPartitionNum = topicDescription.partitions().size();
					if (partitionNum > oldPartitionNum) {
						// add partition
						kafkaAdmin.addPartitionsForTopic(topicName, partitionNum);
						topicDescription = kafkaAdmin.getDescriptionByTopicName(topicName);
					}
				} else {
					// create topic
					kafkaAdmin.createTopic(topicName, partitionNum, replicationFactor);
					topicDescription = kafkaAdmin.getDescriptionByTopicName(topicName);
				}
				
				// 
				if ( topicDescription == null) {
					topicCfg.setMetadata( null );
					return;
					
				} else {
				
					String name = topicDescription.name();
					boolean internal = topicDescription.isInternal();
					int partitionSize = topicDescription.partitions().size();
					
					//
					DataPartition[] newPartitions = new DataPartition[ partitionSize ];
					for (int i = 0; i < partitionSize; i++) {
						
						TopicPartitionInfo partitionInfo =  topicDescription.partitions().get(i);
						int partition = partitionInfo.partition();
						
						Node leader = partitionInfo.leader();
						DataNode newLeader = new DataNode(leader.id(), leader.host(), leader.port());
						
						List<Node> replicas = partitionInfo.replicas();
						DataNode[] newReplicas = new DataNode[replicas.size()];
						for (int j = 0; j < replicas.size(); j++) {
							newReplicas[j] = new DataNode(replicas.get(j).id(), replicas.get(j).host(), replicas.get(j).port());
						}
						
						DataPartition newPartition = new DataPartition(partition, newLeader, newReplicas);
						newPartitions[i] = newPartition;
					}
	
					Metadata metadata = new Metadata(name, internal, newPartitions);
					topicCfg.setMetadata( metadata );
				}

			}
		} catch(Throwable e) {
			LOGGER.error("load kafka err:", e);
			
		} finally {
			if (kafkaAdmin != null)
				kafkaAdmin.close();
		}
	}


	
	// 重新加载
	public byte[] reloadAll() {
		try {
			Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
			for (Entry<Integer, PoolCfg> entry : poolCfgMap.entrySet()) {
				PoolCfg poolCfg = entry.getValue();
				if ( poolCfg instanceof KafkaPoolCfg )
					poolCfg.reloadExtraCfg();
			}
			
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		} finally {
			lock.unlock();
		}

		return "+OK\r\n".getBytes();
	}
}
