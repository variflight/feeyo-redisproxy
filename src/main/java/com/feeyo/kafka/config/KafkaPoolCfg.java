package com.feeyo.kafka.config;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.config.loader.KafkaCtx;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;

public class KafkaPoolCfg extends PoolCfg {
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaPoolCfg.class );
	
	// topicName -> topicCfg
	private Map<String, TopicCfg> topicCfgMap = null;

	public KafkaPoolCfg(int id, String name, int type, int minCon, int maxCon) {
		super(id, name, type, minCon, maxCon);
	}
	
	// 加载kafka配置
	public boolean load() {
		try {
			
			// load topic
			topicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
			
			// load topic metadata
			KafkaCtx.getInstance().load(topicCfgMap, this);
			
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return true;
	}
	
	public boolean reload() throws Exception {
		
		try {
			Map<String, TopicCfg> newTopicCfgMap = KafkaConfigLoader.loadTopicCfgMap(this.id, ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
			
			// load topic metadata
			KafkaCtx.getInstance().load(newTopicCfgMap, this);
			
			for (Entry<String, TopicCfg> entry : newTopicCfgMap.entrySet()) {
				String key = entry.getKey();
				TopicCfg newTopicCfg = entry.getValue();
				TopicCfg oldTopicCfg = topicCfgMap.get(key);
				if (oldTopicCfg != null) {
					// 迁移原来的offset
					newTopicCfg.getMetadata().setDataOffsets(oldTopicCfg.getMetadata().getDataOffsets());

					// 新建的topic
				} else {
					Map<Integer, DataOffset> dataOffsets = new ConcurrentHashMap<Integer, DataOffset>();

					for (DataPartition partition : newTopicCfg.getMetadata().getPartitions()) {
						DataOffset dataOffset = new DataOffset(partition.getPartition(), 0, 0);
						dataOffsets.put(partition.getPartition(), dataOffset);
					}

					newTopicCfg.getMetadata().setDataOffsets(dataOffsets);
				}
			}
			
			
		} catch (Exception e) {
			LOGGER.error("", e);
			throw e;
		}
		
		return true;
	}

	public Map<String, TopicCfg> getTopicCfgMap() {
		return topicCfgMap;
	}
}
