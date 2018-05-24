package com.feeyo.kafka.net.backend.broker.offset;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.OffsetCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.BrokerPartitionOffset;
import com.feeyo.kafka.net.backend.broker.ConsumerOffset;
import com.feeyo.kafka.net.backend.broker.zk.ZkClientx;
import com.feeyo.kafka.net.backend.broker.zk.ZkPathUtil;
import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

/**
 * 管理 topic offset
 * 
 * @author yangtao
 */
public class LocalOffsetAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger(LocalOffsetAdmin.class);

	private String zkServerIp;
	private ZkPathUtil zkPathUtil;

	public LocalOffsetAdmin(OffsetCfg offsetCfg) {
		this.zkServerIp = offsetCfg.getZkServerIp();
		this.zkPathUtil = new ZkPathUtil( offsetCfg.getPath() );
	}


	private void loadOffsetByPoolId(int poolId, Collection<TopicCfg> topicCfgs) {
		
		ZkClientx zkclientx = ZkClientx.getZkClient( zkServerIp );
		
		for (TopicCfg topicCfg : topicCfgs) {
			
			String topic = topicCfg.getName();
			
			ConcurrentHashMap<Integer, BrokerPartitionOffset> partitionOffsetMap = new ConcurrentHashMap<Integer, BrokerPartitionOffset>();
			try {
				for (BrokerPartition partition : topicCfg.getRunningOffset().getBrokerPartitions()) {
					
					// log_start_offset
					String partitionLogStartOffsetPath = zkPathUtil.getPartitionLogStartOffsetPath(poolId, topic, partition.getPartition());
					String data = readDataByPath(zkclientx, partitionLogStartOffsetPath);
					long logStartOffset = isNull(data) ? 0 : Long.parseLong(data);
					
					// produce_offset
					String partitionProducerOffsetPath = zkPathUtil.getPartitionProducerOffsetPath(poolId, topic, partition.getPartition());
					data = readDataByPath(zkclientx, partitionProducerOffsetPath);
					long producerOffset = isNull(data) ? 0 : Long.parseLong(data);
					
					//
					BrokerPartitionOffset partitionOffset = new BrokerPartitionOffset(partition.getPartition(), producerOffset, logStartOffset);
					partitionOffsetMap.put(partition.getPartition(), partitionOffset);
					
					// consumer offset
					String partitionConsumerPath = zkPathUtil.getPartitionConsumerPath(poolId, topic, partition.getPartition());
					if (!zkclientx.exists(partitionConsumerPath)) {
						continue;
					}
					
					//
					List<String> childrenPath = zkclientx.getChildren(partitionConsumerPath);
					for (String consumer : childrenPath) {
						// consumer_offset
						String partitionConsumerOffsetPath = zkPathUtil.getPartitionConsumerOffsetPath(poolId, topic, partition.getPartition(), consumer);
						data = readDataByPath(zkclientx, partitionConsumerOffsetPath);
						long consumerOffset = isNull(data) ? 0 : Long.parseLong(data);
						
						ConsumerOffset co = new ConsumerOffset(consumer, consumerOffset);
						partitionOffset.getConsumerOffsets().put(consumer, co);
						
						// consumer_offset
						String partitionConsumerRollbackOffsetPath = zkPathUtil.getPartitionConsumerRollbackOffsetPath(poolId, topic, partition.getPartition(), consumer);
						data = readDataByPath(zkclientx, partitionConsumerRollbackOffsetPath);
						if (!isNull(data)) {
							ConcurrentLinkedQueue<?> offsets = JsonUtils.unmarshalFromString(data, ConcurrentLinkedQueue.class);
							Object object = offsets.poll();
							while (object != null) {
								if (object instanceof Integer) {
									co.getOldOffsetQueue().offer( Long.parseLong(object.toString()) );
									
								} else if (object instanceof Long) {
									co.getOldOffsetQueue().offer( (long) object );
								}
								object = offsets.poll();
							}
						}
					}
				}

				topicCfg.getRunningOffset().setPartitionOffsets(partitionOffsetMap);

			} catch (Exception e) {
				LOGGER.warn("", e);
			}
		}
	}

	/**
	 * 启动
	 */
	public void startup() {

		final Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		for (PoolCfg poolCfg : poolCfgMap.values()) {
			if (poolCfg instanceof KafkaPoolCfg) {
				
				Map<String, TopicCfg> topicCfgMap = ((KafkaPoolCfg) poolCfg).getTopicCfgMap();
				this.loadOffsetByPoolId(poolCfg.getId(), topicCfgMap.values());
			}
		}
	}

	/**
	 * 关闭
	 */
	public void close() {
	}

	
	/**
	 * offsets 持久化
	 */
	public void flushAll() {
		
		final Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		for (Entry<Integer, PoolCfg> poolEntry : poolCfgMap.entrySet()) {
			PoolCfg poolCfg = poolEntry.getValue();
			if (poolCfg instanceof KafkaPoolCfg) {

				//
				ZkClientx zkclientx = ZkClientx.getZkClient( zkServerIp );
				
				Map<String, TopicCfg> topicCfgMap = ((KafkaPoolCfg) poolCfg).getTopicCfgMap();
				for (Entry<String, TopicCfg> topicEntry : topicCfgMap.entrySet()) {
					TopicCfg topicCfg = topicEntry.getValue();
					int poolId = poolCfg.getId();
					String topicName = topicCfg.getName();
					
				
					try {
						
						for (BrokerPartitionOffset partitionOffset : topicCfg.getRunningOffset().getPartitionOffsets().values()) {
							
							int partition = partitionOffset.getPartition();
							
							// log_start_offset
							String partitionLogStartOffsetPath = zkPathUtil.getPartitionLogStartOffsetPath(poolId, topicName, partition);
							writeDataByPath(zkclientx, partitionLogStartOffsetPath, String.valueOf(partitionOffset.getLogStartOffset()));
							
							// produce_offset
							String partitionProducerOffsetPath = zkPathUtil.getPartitionProducerOffsetPath(poolId, topicName, partition);
							writeDataByPath(zkclientx, partitionProducerOffsetPath, String.valueOf(partitionOffset.getProducerOffset()));
							
							// 消费者
							Map<String, ConsumerOffset> consumerOffsets = partitionOffset.getConsumerOffsets();
							for (ConsumerOffset consumeOffset : consumerOffsets.values()) {
								String consumer = consumeOffset.getConsumer();
								
								// 消费者点位
								String partitionConsumerOffsetPath = zkPathUtil.getPartitionConsumerOffsetPath(poolId, topicName, partition, consumer);
								writeDataByPath(zkclientx, partitionConsumerOffsetPath, String.valueOf(consumeOffset.getCurrentOffset()));
								
								// 消费者回退点位
								String partitionConsumerRollbackOffsetPath = zkPathUtil.getPartitionConsumerRollbackOffsetPath(poolId, topicName, partition, consumer);
								writeDataByPath(zkclientx, partitionConsumerRollbackOffsetPath, consumeOffset.getOldOffsetQueue().toString());

							}
						}
					} catch (Exception e) {
						LOGGER.warn("offset flush err:", e);
					}

				}
			}
		}
	}

	private boolean isNull(String str) {
		if (str == null) {
			return true;
		}

		if ("".equals(str) || "null".equals(str) || "NULL".equals(str)) {
			return true;
		}

		return false;
	}


	// 申请offset
	public long getOffset(String user, TopicCfg topicCfg, int partition) {
		
		BrokerPartitionOffset partitionOffset = topicCfg.getRunningOffset().getPartitionOffset(partition);
		ConsumerOffset cOffset = partitionOffset.getConsumerOffsetByConsumer(user);
		long offset = cOffset.getNewOffset();
		return offset;
	}

	// 返还offset
	public void returnOffset(String user, String topic, int partition, long offset) {

		UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
		if (userCfg == null) {
			return;
		}
		
		KafkaPoolCfg poolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get(userCfg.getPoolId());
		if (poolCfg == null) {
			return;
		}
		
		TopicCfg topicCfg = poolCfg.getTopicCfgMap().get(topic);
		if (topicCfg == null) {
			return;
		}
		
		BrokerPartitionOffset partitionOffset = topicCfg.getRunningOffset().getPartitionOffset(partition);
		if (partitionOffset != null) {
			partitionOffset.rollbackConsumerOffset(user, offset);
		}
	}

	// 更新生产offset
	public void updateProducerOffset(String user, String topic, int partition, long offset, long logStartOffset) {

		UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
		if (userCfg == null) {
			return;
		}
		
		KafkaPoolCfg poolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get(userCfg.getPoolId());
		if (poolCfg == null) {
			return;
		}
		
		TopicCfg topicCfg = poolCfg.getTopicCfgMap().get(topic);
		if (topicCfg == null) {
			return;
		}
		
		BrokerPartitionOffset partitionOffset = topicCfg.getRunningOffset().getPartitionOffset(partition);
		if (partitionOffset != null) {
			partitionOffset.setProducerOffset(offset, logStartOffset);
		}
	}
	
	
	
	// 
	private String readDataByPath(ZkClientx zkclientx, String path) {
		
		if ( !zkclientx.exists(path) ) {
			zkclientx.createPersistent(path, null, true);
		}
		
		Object obj = zkclientx.readData(path);
		if (obj == null) {
			return null;
		}
		
		if (obj instanceof String) {
			return (String) obj;
		} else {
			return new String((byte[]) obj);
		}
	}
	
	private void writeDataByPath(ZkClientx zkclientx, String path, String value) {
		if (!zkclientx.exists(path)) {
			zkclientx.createPersistent(path, null, true);
		}
		zkclientx.writeData(path, value);
	}
}
