package com.feeyo.kafka.net.backend.broker.offset;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.BrokerPartition.ConsumerOffset;
import com.feeyo.kafka.net.backend.broker.zk.ZkClientx;
import com.feeyo.kafka.net.backend.broker.zk.util.ZkPathUtil;
import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

/**
 * 管理 topic offset
 * 
 * @author yangtao
 */
public class OffsetLocalAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger(OffsetLocalAdmin.class);

	private String zkServerIp;
	private ZkPathUtil zkPathUtil;

	public OffsetLocalAdmin(String zkServerIp, String rootPath) {
		this.zkServerIp = zkServerIp;
		this.zkPathUtil = new ZkPathUtil( rootPath );
	}


	private void recoveryFromZkByPoolId(int poolId, Collection<TopicCfg> topicCfgs) {
		
		
		LOGGER.info("recoveryFromZkByPoolId: poolId={}", poolId);
		
		ZkClientx zkclientx = ZkClientx.getZkClient( zkServerIp );
		
		for (TopicCfg topicCfg : topicCfgs) {

			try {
				
				//
				for (BrokerPartition p : topicCfg.getRunningInfo().getPartitions().values()) {
					
					
					//  producer & log_start
					//
					String partitionLogStartOffsetPath = zkPathUtil.getPartitionLogStartOffsetPath(poolId, topicCfg.getName(), p.getPartition());
					String logStartOffsetData = readDataByPath(zkclientx, partitionLogStartOffsetPath);
					long logStartOffset = isNull(logStartOffsetData) ? 0 : Long.parseLong(logStartOffsetData);
					
					String partitionProducerOffsetPath = zkPathUtil.getPartitionProducerOffsetPath(poolId, topicCfg.getName(), p.getPartition());
					String producerOffsetData = readDataByPath(zkclientx, partitionProducerOffsetPath);
					long producerOffset = isNull(producerOffsetData) ? 0 : Long.parseLong(producerOffsetData);
					
					// load producer & log_start
					p.setProducerOffset(producerOffset);
					p.setLogStartOffset(logStartOffset);
					
					
					// consumer 
					//
					String consumerPath = zkPathUtil.getPartitionConsumerPath(poolId, topicCfg.getName(), p.getPartition());
					if (!zkclientx.exists(consumerPath)) {
						continue;
					}
					
					//
					List<String> childrenPath = zkclientx.getChildren(consumerPath);
					for (String consumer : childrenPath) {
						
						// consumer offset
						String consumerOffsetPath = zkPathUtil.getPartitionConsumerOffsetPath(poolId, topicCfg.getName(), p.getPartition(), consumer);
						String consumerOffsetData = readDataByPath(zkclientx, consumerOffsetPath);
						long offset = isNull( consumerOffsetData ) ? 0 : Long.parseLong( consumerOffsetData );
						
						// load consumer offset
						ConsumerOffset consumerOffset = new ConsumerOffset(consumer, offset);
						p.addConsumerOffset( consumerOffset );
						
						// consumer return offset
						String consumerReturnOffsetPath = zkPathUtil.getPartitionConsumerReturnOffsetPath(poolId, topicCfg.getName(), p.getPartition(), consumer);
						String consumerReturnOffsetData = readDataByPath(zkclientx, consumerReturnOffsetPath);
						if ( !isNull( consumerReturnOffsetData ) ) {
							ConcurrentLinkedQueue<?> offsets = JsonUtils.unmarshalFromString(consumerReturnOffsetData, ConcurrentLinkedQueue.class);
							Object object = offsets.poll();
							while (object != null) {
								if (object instanceof Integer) {
									consumerOffset.getOldOffsetQueue().offer( Long.parseLong(object.toString()) );
									
								} else if (object instanceof Long) {
									consumerOffset.getOldOffsetQueue().offer( (long) object );
								}
								object = offsets.poll();
							}
						}
					}
				}

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
				this.recoveryFromZkByPoolId(poolCfg.getId(), topicCfgMap.values());
				
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
		for (PoolCfg poolCfg: poolCfgMap.values()) {

			if (poolCfg instanceof KafkaPoolCfg) {

				//
				ZkClientx zkclientx = ZkClientx.getZkClient( zkServerIp );
				
				Map<String, TopicCfg> topicCfgMap = ((KafkaPoolCfg) poolCfg).getTopicCfgMap();
				for (Entry<String, TopicCfg> topicEntry : topicCfgMap.entrySet()) {
					TopicCfg topicCfg = topicEntry.getValue();
					int poolId = poolCfg.getId();
					String topicName = topicCfg.getName();
					
				
					try {
						
						for (BrokerPartition p : topicCfg.getRunningInfo().getPartitions().values()) {
							
							int partition = p.getPartition();
							
							// log_start_offset
							String partitionLogStartOffsetPath = zkPathUtil.getPartitionLogStartOffsetPath(poolId, topicName, partition);
							writeDataByPath(zkclientx, partitionLogStartOffsetPath, String.valueOf(p.getLogStartOffset()));
							
							// produce_offset
							String partitionProducerOffsetPath = zkPathUtil.getPartitionProducerOffsetPath(poolId, topicName, partition);
							writeDataByPath(zkclientx, partitionProducerOffsetPath, String.valueOf(p.getProducerOffset()));
							
							// 消费者
							Map<String, ConsumerOffset> consumerOffsets = p.getConsumerOffsets();
							for (ConsumerOffset consumeOffset : consumerOffsets.values()) {
								String consumer = consumeOffset.getConsumer();
								
								// 消费者点位
								String partitionConsumerOffsetPath = zkPathUtil.getPartitionConsumerOffsetPath(poolId, topicName, partition, consumer);
								writeDataByPath(zkclientx, partitionConsumerOffsetPath, String.valueOf(consumeOffset.getCurrentOffset()));
								
								// 消费者回退点位
								String partitionConsumerRollbackOffsetPath = zkPathUtil.getPartitionConsumerReturnOffsetPath(poolId, topicName, partition, consumer);
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
		BrokerPartition brokerPartition = topicCfg.getRunningInfo().getPartition(partition);
		ConsumerOffset cOffset = brokerPartition.getConsumerOffset(user);
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
		
		BrokerPartition brokerPartition = topicCfg.getRunningInfo().getPartition(partition);
		if (brokerPartition != null) {
			brokerPartition.returnConsumerOffset(user, offset);
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
		
		BrokerPartition brokerPartition = topicCfg.getRunningInfo().getPartition(partition);
		if (brokerPartition != null) {
			brokerPartition.setProducerOffset(offset, logStartOffset);
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