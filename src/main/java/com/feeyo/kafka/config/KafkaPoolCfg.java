package com.feeyo.kafka.config;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.admin.KafkaAdmin;
import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.net.backend.broker.BrokerNode;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.BrokerRunningInfo;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

public class KafkaPoolCfg extends PoolCfg {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaPoolCfg.class );
	
	// topicName -> topicCfg
	private volatile Map<String, TopicCfg> topicCfgMap = null;

	public KafkaPoolCfg(int id, String name, int type, int minCon, int maxCon, float maxLatencyThreshold) {
		super(id, name, type, minCon, maxCon, maxLatencyThreshold);
	}
	
	@Override
	public void loadExtraCfg() throws Exception {
		
		// 加载 kafka xml
		this.topicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		this.initializeOfKafka( topicCfgMap );
	}
	
	@Override
	public void reloadExtraCfg() throws Exception {
		
		// 1、 加载新的配置 
		Map<String, TopicCfg> newTopicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		
		// 校验生产者消费者
		Map<String, UserCfg> userMap = RedisEngineCtx.INSTANCE().getUserMap();
		for (TopicCfg tc : newTopicCfgMap.values()) {
			for (String producer : tc.getProducers()) {
				UserCfg uc = userMap.get(producer);
				if (uc == null || uc.getPoolType() != 3) {
					LOGGER.error(tc.getName() + ": kafka producer can not be a user who is not exists or not in the kakfa pool");
					throw new Exception(tc.getName() + ": kafka producer can not be a user who is not exists or not in the kakfa pool");
				}
			}
			
			for (String consumer : tc.getConsumers()) {
				UserCfg uc = userMap.get(consumer);
				if (uc == null || uc.getPoolType() != 3) {
					LOGGER.error(tc.getName() + ": kafka consumer can not be a user who is not exists or not in the kakfa pool");
					throw new Exception(tc.getName() + ": kafka consumer can not be a user who is not exists or not in the kakfa pool");
				}
			}
		}
		
		this.initializeOfKafka( newTopicCfgMap );
		
		// 2、增加旧引用
		Map<String, TopicCfg> oldTopicCfgMap = topicCfgMap;
		
		// 3、新旧对比
		for (TopicCfg newTopicCfg : newTopicCfgMap.values()) {
			
			//
			TopicCfg oldTopicCfg = oldTopicCfgMap.get( newTopicCfg.getName() );
			if ( oldTopicCfg != null) {
				
				//
				for(BrokerPartition newPartition: newTopicCfg.getRunningInfo().getPartitions().values() ) {
					
					BrokerPartition oldPartition = oldTopicCfg.getRunningInfo().getPartition( newPartition.getPartition() );
					if ( oldPartition != null ) {
						newPartition.setProducerConsumerOffset(  oldPartition.getProducerConsumerOffset() );

					} else {
						// TODO
						// delete old partition
					}
				}
			}
		}
		
		// 4、交互
		this.topicCfgMap = newTopicCfgMap;
		
		// 5、旧对象清理
	}
	
	/**
	 * 
	 * 
		zhuamdeMacBook-Pro:logs zhuam$ [2018-05-28 15:26:41,394] INFO [Admin Manager on Broker 0]: 
		Error processing create topic request for topic test01 with arguments (numPartitions=3, replicationFactor=2, replicasAssignments={}, configs={}) (kafka.server.AdminManager)
		org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 2 larger than available brokers: 1.
	 */
	private void initializeOfKafka(Map<String, TopicCfg> topicCfgMap) throws Exception, org.apache.kafka.common.errors.TimeoutException {
		
		if (topicCfgMap == null || topicCfgMap.isEmpty()) {
			return;
		}
		
		// Get server address for kafka
		StringBuffer servers = new StringBuffer();
		List<String> nodes = this.getNodes();
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
			kafkaAdmin = KafkaAdmin.create( servers.toString() );
			
			// 获取kafka中的topic情况
			Map<String, TopicDescription> remoteTopics = kafkaAdmin.getTopicAndDescriptions();
			Collection<Node> clusterNodes = kafkaAdmin.getClusterNodes();
			for (TopicCfg topicCfg : topicCfgMap.values()) {

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
					//verify
					if(clusterNodes == null || replicationFactor > clusterNodes.size()) {
						throw new Exception( "kafka topicName="+ topicName + ", no enough alive physical nodes for replication");
					}
					
					// create topic
					kafkaAdmin.createTopic(topicName, partitionNum, replicationFactor);
					topicDescription = kafkaAdmin.getDescriptionByTopicName(topicName);
				}
				
				// 
				if ( topicDescription == null) {
					throw new Exception( " kafka topicName=" + topicName  + ", description is null.");
				} 

				//
				String name = topicDescription.name();
				boolean internal = topicDescription.isInternal();
				int partitionSize = topicDescription.partitions().size();
				
				//
				BrokerPartition[] newPartitions = new BrokerPartition[ partitionSize ];
				for (int i = 0; i < partitionSize; i++) {
					
					TopicPartitionInfo partitionInfo =  topicDescription.partitions().get(i);
					int partition = partitionInfo.partition();
					
					Node leader = partitionInfo.leader();
					BrokerNode newLeader = new BrokerNode(leader.id(), leader.host(), leader.port());
					
					List<Node> replicas = partitionInfo.replicas();
					BrokerNode[] newReplicas = new BrokerNode[replicas.size()];
					for (int j = 0; j < replicas.size(); j++) {
						newReplicas[j] = new BrokerNode(replicas.get(j).id(), replicas.get(j).host(), replicas.get(j).port());
					}
					
					BrokerPartition newPartition = new BrokerPartition(partition, newLeader, newReplicas);
					newPartitions[i] = newPartition;
				}

				topicCfg.setRunningInfo( new BrokerRunningInfo(name, internal, newPartitions) );
			}
			
		} catch(Throwable e) {
			throw new Exception("kafka pool init err: " + servers.toString(), e);
			
		} finally {
			if (kafkaAdmin != null)
				kafkaAdmin.close();
		}
	}
	

	public Map<String, TopicCfg> getTopicCfgMap() {
		return topicCfgMap;
	}
	
}
