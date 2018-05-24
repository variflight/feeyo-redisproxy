package com.feeyo.kafka.config;

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
import com.feeyo.kafka.net.backend.broker.offset.KafkaOffsetService;
import com.feeyo.kafka.net.backend.broker.offset.RunningOffset;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;

public class KafkaPoolCfg extends PoolCfg {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaPoolCfg.class );
	
	// topicName -> topicCfg
	private Map<String, TopicCfg> topicCfgMap = null;

	public KafkaPoolCfg(int id, String name, int type, int minCon, int maxCon) {
		super(id, name, type, minCon, maxCon);
	}
	
	@Override
	public void loadExtraCfg() throws Exception {
		
        // 加载 offset service
		if ( !KafkaOffsetService.INSTANCE().isRunning() ) {
			
			KafkaOffsetService.INSTANCE().start();
	        Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					KafkaOffsetService.INSTANCE().close();
				}
			});
		}
		
		// 加载 kafka xml
		this.topicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		this.initializeOfKafka( topicCfgMap );
	}
	
	@Override
	public void reloadExtraCfg() throws Exception {
		
		// 1、 加载新的配置 
		Map<String, TopicCfg> newTopicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		this.initializeOfKafka( newTopicCfgMap );
		
		// 2、增加旧引用
		Map<String, TopicCfg> oldTopicCfgMap = topicCfgMap;
		
		// 3、新旧对比
		for (TopicCfg newTopicCfg : newTopicCfgMap.values()) {
			
			//
			TopicCfg oldTopicCfg = oldTopicCfgMap.get( newTopicCfg.getName() );
			if ( oldTopicCfg != null) {
				
				//
				for(BrokerPartition newPartition: newTopicCfg.getRunningOffset().getPartitions().values() ) {
					
					BrokerPartition oldPartition = oldTopicCfg.getRunningOffset().getPartition( newPartition.getPartition() );
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
	
	
	private void initializeOfKafka(Map<String, TopicCfg> topicCfgMap) throws Exception {
		
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

				topicCfg.setRunningOffset( new RunningOffset(name, internal, newPartitions) );
			}
			
		} catch(Throwable e) {
			LOGGER.error("initialize kafka err:", e);
			throw e;
			
		} finally {
			if (kafkaAdmin != null)
				kafkaAdmin.close();
		}
	}
	

	public Map<String, TopicCfg> getTopicCfgMap() {
		return topicCfgMap;
	}
	
}
