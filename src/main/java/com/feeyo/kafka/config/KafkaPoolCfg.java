package com.feeyo.kafka.config;

import java.util.Map;

import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.config.loader.KafkaCtx;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;

public class KafkaPoolCfg extends PoolCfg {
	
	// topicName -> topicCfg
	private Map<String, TopicCfg> topicCfgMap = null;

	public KafkaPoolCfg(int id, String name, int type, int minCon, int maxCon) {
		super(id, name, type, minCon, maxCon);
	}
	
	@Override
	public void loadExtraCfg() throws Exception {
		
		// 加载kafka配置
		topicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		KafkaCtx.getInstance().load(topicCfgMap, this);
	}
	
	@Override
	public void reloadExtraCfg() throws Exception {
		
		// 1、 加载新的配置 
		Map<String, TopicCfg> newTopicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
		KafkaCtx.getInstance().load(newTopicCfgMap, this);
		
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

	public Map<String, TopicCfg> getTopicCfgMap() {
		return topicCfgMap;
	}
}
