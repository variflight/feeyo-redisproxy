package com.feeyo.kafka.admin;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.feeyo.kafka.config.ConsumerOffset;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.DataOffset;
import com.feeyo.kafka.config.DataPartition;
import com.feeyo.kafka.config.OffsetManageCfg;
import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.engine.RedisEngineCtx;

/**
 * 管理 topic offset
 * 
 * @author yangtao
 */
public class OffsetAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger(OffsetAdmin.class);
	
	private static final String ZK_CFG_FILE = "kafka.xml"; // zk settings is in server.xml
	

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	private static OffsetAdmin INSTANCE = new OffsetAdmin();

	private CuratorFramework curator;
	private OffsetManageCfg offsetManageCfg;
	
	
	public static OffsetAdmin getInstance() {
		return INSTANCE;
	}
	
	private OffsetAdmin() {

		offsetManageCfg = KafkaConfigLoader.loadOffsetManageCfg(ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE));
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(offsetManageCfg.getServer())
				.retryPolicy(new RetryNTimes(3, 1000)).connectionTimeoutMs(3000);

		curator = builder.build();

		curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState state) {
				switch (state) {
				case CONNECTED:
					LOGGER.info("connected with zookeeper");
					break;
				case LOST:
					LOGGER.warn("lost session with zookeeper");
					break;
				case RECONNECTED:
					LOGGER.warn("reconnected with zookeeper");
					break;
				default:
					break;
				}
			}
		});
		curator.start();
		
		INSTANCE = this;
	}
	
	/**
	 * 创建节点
	 * @param path
	 */
	private void createZkNode(String path, byte[] data) {
		try {
			if (curator.checkExists().forPath(path) != null) {
				return;
			}
			int index = path.lastIndexOf('/');
			if (index > 0) {
				createZkNode(path.substring(0, index), null);
			}
			curator.create().forPath(path, data);
		} catch (Exception e) {
			LOGGER.warn("", e);
		}
	}
	
	/**
	 * 加载offset
	 */
	@SuppressWarnings("unchecked")
	private void load() {
		Map<String, TopicCfg> kafkaMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
		
		for (Entry<String, TopicCfg> entry : kafkaMap.entrySet()) {
			TopicCfg kafkaCfg = entry.getValue();
			String topic  = kafkaCfg.getTopic();
			String path = offsetManageCfg.getPath() + File.separator + topic;
			try {
				// base node 
				createZkNode(path, null);
				
				Map<Integer, DataOffset> metaDataOffsets = new ConcurrentHashMap<Integer, DataOffset>();
				byte[] data = curator.getData().forPath(path);
				if (data == null) {
					
					for (DataPartition partition : kafkaCfg.getMetaData().getPartitions()) {
						DataOffset metaDataOffset = new DataOffset(partition.getPartition(), 0, 0);
						metaDataOffsets.put(partition.getPartition(), metaDataOffset);
					}
					setTopicOffsets(topic, metaDataOffsets);
					
				} else {
					
					/*
					 {
					 	2:{"offsets":{"pwd01":{"consumer":"pwd01","offset":1},"pwd02":{"consumer":"pwd02","offset":2}},"partition":1,"producerOffset":100}
					  	1:{"offsets":{"pwd01":{"consumer":"pwd01","offset":1},"pwd02":{"consumer":"pwd02","offset":2}},"partition":1,"producerOffset":100}
					  }
					*/

					String str = new String(data);
					JSONObject obj = JsonUtils.unmarshalFromString(str, JSONObject.class);
					
					for (DataPartition partition : kafkaCfg.getMetaData().getPartitions()) {
						DataOffset metaDataOffset;
						Object metaDataOffsetObject = obj.get(String.valueOf(partition.getPartition()));
						if (metaDataOffsetObject == null) {
							metaDataOffset = new DataOffset(partition.getPartition(), 0, 0);
						} else {
							JSONObject metaDataOffsetJSONObject = JsonUtils.unmarshalFromString(String.valueOf(metaDataOffsetObject), JSONObject.class);
							metaDataOffset = new DataOffset(partition.getPartition(),
									metaDataOffsetJSONObject.get("producerOffset") == null ? 0
											: Integer.parseInt(metaDataOffsetJSONObject.getString("producerOffset")),
											metaDataOffsetJSONObject.get("logStartOffset") == null ? 0
													: Integer.parseInt(metaDataOffsetJSONObject.getString("logStartOffset")));
							Map<String, ConsumerOffset> offsets = new ConcurrentHashMap<String, ConsumerOffset>();
							
							JSONObject offsetsObject = JsonUtils.unmarshalFromString(metaDataOffsetJSONObject.getString("offsets"),
									JSONObject.class);
							HashSet<String> consumers = kafkaCfg.getConsumers();
							for (String consumer : consumers) {
								ConsumerOffset co;
								if (offsetsObject.get(consumer) != null) {
									JSONObject consumerOffsetObj = JsonUtils.unmarshalFromString(offsetsObject.getString(consumer),
											JSONObject.class);
									co = new ConsumerOffset(consumer, consumerOffsetObj.getString("offset") == null ? 0
											: Integer.parseInt(consumerOffsetObj.getString("offset")));
									
									if (consumerOffsetObj.get("defaultOffset") != null) {
										List<Object> defaultOffsets = JsonUtils.unmarshalFromString(consumerOffsetObj.getString("defaultOffset"), List.class);
										for (Object defaultOffset : defaultOffsets) {
											co.offerOffset(Long.parseLong(String.valueOf(defaultOffset)));
										}
									}
									
								} else {
									co = new ConsumerOffset(consumer, 0);
								}
								offsets.put(consumer, co);
							}
							metaDataOffset.setOffsets(offsets);
						}
						metaDataOffsets.put(partition.getPartition(), metaDataOffset);
					}
					
				}
				
				kafkaCfg.getMetaData().setOffsets(metaDataOffsets);
				
			} catch (Exception e) {
				LOGGER.warn("", e);
			}
		}
		
	}
	
	/**
	 * commit offset
	 * @param topic
	 * @param offset
	 */
	private void setTopicOffsets(String topic,  Map<Integer, DataOffset> offset) {
		String path = offsetManageCfg.getPath() + File.separator + topic;
		Stat stat;
		try {
			stat = curator.checkExists().forPath(path);
			if (stat == null) {
				ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
			}
			
			curator.setData().inBackground().forPath(path, JsonUtils.marshalToByte(offset));
		} catch (Exception e) {
			LOGGER.warn("kafka cmd offset commit err:", e);
		}
	}

	public void startUp() {
		
		this.load();
		
		// 定时持久化offset
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					// offset 数据持久化
					Map<String, TopicCfg> kafkaMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
					for (Entry<String, TopicCfg> entry : kafkaMap.entrySet()) {
						TopicCfg kafkaCfg = entry.getValue();
						setTopicOffsets(kafkaCfg.getTopic(), kafkaCfg.getMetaData().getOffsets());
					}
				} catch (Exception e) {
					LOGGER.warn("offset admin warn", e);
				}
				
			}
		}, 30, 30, TimeUnit.SECONDS);
	}
	
	public void close() {
		
		// 关闭定时任务
		executorService.shutdown();
		
		// 停止获取新的offset 
		Map<String, TopicCfg> kafkaMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
		for (Entry<String, TopicCfg> entry : kafkaMap.entrySet()) {
			TopicCfg kafkaCfg = entry.getValue();
			kafkaCfg.getMetaData().close();
		}
		
		// 提交本地剩余offset
		for (Entry<String, TopicCfg> entry : kafkaMap.entrySet()) {
			TopicCfg kafkaCfg = entry.getValue();
			setTopicOffsets(kafkaCfg.getTopic(), kafkaCfg.getMetaData().getOffsets());
		}
	}
}
