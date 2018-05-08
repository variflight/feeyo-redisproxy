package com.feeyo.kafka.config.loader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.admin.KafkaAdmin;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.Metadata;
import com.feeyo.kafka.config.DataNode;
import com.feeyo.kafka.config.DataOffset;
import com.feeyo.kafka.config.DataPartition;
import com.feeyo.kafka.net.backend.callback.ApiVersionCallback;
import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.util.Utils;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.nio.NetSystem;

public class KafkaCtx {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaCtx.class);

	private final static KafkaCtx INSTANCE = new KafkaCtx();
	
	private ReentrantLock lock = new ReentrantLock();
	
	private KafkaCtx() {
	}

	public static KafkaCtx getInstance() {
		return INSTANCE;
	}

	public void load(Map<String, TopicCfg> topicCfgMap) {
		
		if (topicCfgMap == null || topicCfgMap.isEmpty()) {
			return;
		}
		
		Map<Integer, List<TopicCfg>> topics = groupBy(topicCfgMap);
		for (Entry<Integer, List<TopicCfg>> entry : topics.entrySet()) {
			
			// 获取kafka地址
			int poolId = entry.getKey();
			PoolCfg poolCfg = RedisEngineCtx.INSTANCE().getPoolCfgMap().get(poolId);
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

			// 获取kafka管理对象
			KafkaAdmin kafkaAdmin = new KafkaAdmin(servers.toString());
			Map<String, TopicDescription> existsTopics = kafkaAdmin.getTopicAndDescriptions();
			List<TopicCfg> topicCfgs = entry.getValue();
			for (TopicCfg topicCfg : topicCfgs) {
				if (existsTopics.containsKey(topicCfg.getTopic())) {
					TopicDescription topicDescription = existsTopics.get(topicCfg.getTopic());
					List<TopicPartitionInfo> partitions = topicDescription.partitions();
					// 如果增加分区
					if (partitions.size() < topicCfg.getPartitions()) {
						kafkaAdmin.addPartitionsForTopic(topicCfg.getTopic(), topicCfg.getPartitions());
						topicDescription = kafkaAdmin.getDescriptionByTopic(topicCfg.getTopic());
					}

					initMetadata(topicCfg, topicDescription);
					
				} else {
					kafkaAdmin.createTopic(topicCfg.getTopic(), topicCfg.getPartitions(), topicCfg.getReplicationFactor());
					TopicDescription topicDescription = kafkaAdmin.getDescriptionByTopic(topicCfg.getTopic());
					
					// 初始化metadata
					initMetadata(topicCfg, topicDescription);
				}
			}

			kafkaAdmin.close();
		}

	}

	/**
	 * 初始化kafka配置。metadata
	 * 
	 * @param topicCfg
	 * @param topicDescription
	 */
	private void initMetadata(TopicCfg topicCfg, TopicDescription topicDescription) {
		if (topicDescription == null) {
			topicCfg.setMetadata(null);
			return;
		}
		List<TopicPartitionInfo> partitions = topicDescription.partitions();
		
		DataPartition[] metadataPartitions = new DataPartition[partitions.size()];
		Metadata metadata = new Metadata(topicDescription.name(), topicDescription.isInternal(), metadataPartitions);

		int id = -1;
		for (int i = 0; i < partitions.size(); i++) {
			TopicPartitionInfo topicPartitionInfo = partitions.get(i);
			List<Node> replicas = topicPartitionInfo.replicas();
			DataNode[] replicasData = new DataNode[replicas.size()];
			for (int j = 0; j < replicas.size(); j++) {
				replicasData[j] = new DataNode(replicas.get(j).id(), replicas.get(j).host(),
						replicas.get(j).port());
			}
			
			DataNode leader = new DataNode(topicPartitionInfo.leader().id(), topicPartitionInfo.leader().host(),
					topicPartitionInfo.leader().port());

			DataPartition mdp = new DataPartition(topicPartitionInfo.partition(), leader, replicasData);
			metadataPartitions[i] = mdp;
			id = leader.getId();
		}

		loadApiVersion(topicCfg, id);

		topicCfg.setMetadata(metadata);
	}

	/**
	 * 加载apiversion
	 * @param topicCfg
	 * @param id
	 */
	private void loadApiVersion(TopicCfg topicCfg, int id) {
		
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get(topicCfg.getPoolId());
		PhysicalNode physicalNode = pool.getPhysicalNode(id);
		if (physicalNode != null) {
			try {
				
				RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS.id, (short)1, 
						Thread.currentThread().getName(), Utils.getCorrelationId());
				
				Struct struct = requestHeader.toStruct();
				final ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( struct.sizeOf() + 4 );
				buffer.putInt(struct.sizeOf());
				struct.writeTo(buffer);
				
				//
				AbstractBackendCallback callback = new ApiVersionCallback();
				
				BackendConnection backendCon = physicalNode.getConnection(callback, null);
				if ( backendCon == null ) {
					TodoTask task = new TodoTask() {				
						@Override
						public void execute(BackendConnection backendCon) throws Exception {	
							backendCon.write( buffer );
						}
					};
					callback.addTodoTask(task);
					backendCon = physicalNode.createNewConnection(callback, null);
					
				} else {
					backendCon.write(buffer);
				}
			} catch (IOException e) {
				LOGGER.warn("", e);
			}
		}
	}

	private Map<Integer, List<TopicCfg>> groupBy(Map<String, TopicCfg> topicCfgMap) {
		Map<Integer, List<TopicCfg>> topics = new HashMap<Integer, List<TopicCfg>>();
		for (Entry<String, TopicCfg> entry : topicCfgMap.entrySet()) {
			TopicCfg topicCfg = entry.getValue();
			int poolId = topicCfg.getPoolId();
			
			List<TopicCfg> topicCfgs = topics.get(poolId);
			if (topicCfgs == null) {
				topicCfgs = new ArrayList<TopicCfg>();
				topics.put(poolId, topicCfgs);
			}
			topicCfgs.add(topicCfg);
		}
		return topics;
	}

	// 重新加载
	public byte[] reload() {
		final ReentrantLock lock = this.lock;
		lock.lock();

		Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		Map<String, TopicCfg> kafkaMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
		try {
			// 重新加载kafkamap
			Map<String, TopicCfg> newKafkaMap = KafkaConfigLoader.loadTopicCfgMap(poolCfgMap,
					ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
			load(newKafkaMap);

			for (Entry<String, TopicCfg> entry : newKafkaMap.entrySet()) {
				String key = entry.getKey();
				TopicCfg newKafkaCfg = entry.getValue();
				TopicCfg oldKafkaCfg = kafkaMap.get(key);
				if (oldKafkaCfg != null) {
					// 迁移原来的offset
					newKafkaCfg.getMetadata().setDataOffsets(oldKafkaCfg.getMetadata().getDataOffsets());

					// 新建的topic
				} else {
					Map<Integer, DataOffset> dataOffsets = new ConcurrentHashMap<Integer, DataOffset>();

					for (DataPartition partition : newKafkaCfg.getMetadata().getPartitions()) {
						DataOffset dataOffset = new DataOffset(partition.getPartition(), 0, 0);
						dataOffsets.put(partition.getPartition(), dataOffset);
					}

					newKafkaCfg.getMetadata().setDataOffsets(dataOffsets);
				}
			}
			RedisEngineCtx.INSTANCE().setKafkaTopicMap(newKafkaMap);
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		} finally {
			lock.unlock();
		}

		return "+OK\r\n".getBytes();
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090,localhost:9091,localhost:9092");
		AdminClient adminClient = AdminClient.create(props);

		// 创建topic
		// List<NewTopic> newTopics = new ArrayList<NewTopic>();
		// NewTopic topic = new NewTopic("test2", 1, (short) 1);
		// newTopics.add(topic);
		// CreateTopicsOptions cto = new CreateTopicsOptions();
		// cto.timeoutMs(2000);
		// CreateTopicsResult crt = adminClient.createTopics(newTopics, cto);
		// System.out.println(crt.all().get());

		// 查询topic
		ListTopicsResult ss = adminClient.listTopics();
		// 删除topic
		// adminClient.deleteTopics(ss.names().get());
		// 查询topic配置信息
		DescribeTopicsResult dtr = adminClient.describeTopics(ss.names().get());
		KafkaFuture<Map<String, TopicListing>> a = ss.namesToListings();

		System.out.println(a.get());
		System.out.println(dtr.all().get());
		System.out.println(dtr.all().get().containsKey("test"));
		TopicDescription topicDescription = dtr.all().get().get("test");
		List<TopicPartitionInfo> list = topicDescription.partitions();
		System.out.println(topicDescription);
		System.out.println(list.size());

		// 给topic增加分区数，只能增加不能减少
		Map<String, NewPartitions> map = new HashMap<>();
		NewPartitions x = NewPartitions.increaseTo(1);
		map.put("test2", x);
		// CreatePartitionsResult cpr = adminClient.createPartitions(map);

		// System.out.println(cpr.all().get());

		DescribeClusterOptions dco = new DescribeClusterOptions();
		dco.timeoutMs(5 * 1000);
		DescribeClusterResult dcr = adminClient.describeCluster(dco);
		System.out.println(dcr.nodes());
		System.out.println(dcr.nodes().get());
	}
}
