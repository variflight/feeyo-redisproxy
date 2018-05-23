package com.feeyo.kafka.net.backend.broker.offset;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.OffsetCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.net.backend.broker.zk.ZkClientx;
import com.feeyo.kafka.net.backend.broker.zk.ZkPathUtil;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningData;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningListener;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningMonitor;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

public class KafkaOffsetService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaOffsetService.class );
	
	private static final String ZK_CFG_FILE = "kafka.xml"; // zk settings is in server.xml
	
	private final RemoteOffsetAdmin remoteAdmin;
	private final LocalOffsetAdmin localAdmin;
	
	// 支持 HA
	private ZkClientx  zkclientx;
	private ZkPathUtil zkPathUtil;
	private ServerRunningData runningData;
	private ServerRunningMonitor runningMonitor;	// HA 监控
	
	private String localIp = null;
	
	
	// 刷新
	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	
	private static KafkaOffsetService INSTANCE = new KafkaOffsetService();

	public static KafkaOffsetService INSTANCE() {
		return INSTANCE;
	}
	
	
	private KafkaOffsetService() {
		
		OffsetCfg offsetCfg = KafkaConfigLoader.loadOffsetCfg(ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE));
		this.localAdmin = new LocalOffsetAdmin( offsetCfg );
		this.remoteAdmin = new RemoteOffsetAdmin();
		
		
		this.localIp = offsetCfg.getLocalIp();

		this.zkPathUtil = new ZkPathUtil( offsetCfg.getPath() );
		this.zkclientx = ZkClientx.getZkClient( offsetCfg.getZkServerIp() );
	
		this.runningData = new ServerRunningData( localIp );
		this.runningMonitor = new ServerRunningMonitor( runningData );
		this.runningMonitor.setPath( zkPathUtil.getMasterRunningPath() );
		this.runningMonitor.setListener(new ServerRunningListener() {
			@Override
			public void processStart() {}

			@Override
			public void processStop() {
				localAdmin.close();
			}

			@Override
			public void processActiveEnter() {
				try {
					localAdmin.startup();
				} catch (Exception e) {
					LOGGER.error("offset load err:", e);
				}
			}

			@Override
			public void processActiveExit() {
				localAdmin.close();
			}
			
        });
        
        if ( zkclientx != null) {
            runningMonitor.setZkClient(zkclientx);
        }
        
        // 触发创建一下cid节点
        runningMonitor.init();
	}
	
	public void start() throws IOException {
		
		LOGGER.info("## start offset ha [{}]", localIp);
		
		final String path = zkPathUtil.getClusterHostPath( localIp);
		initCid(path);
		if (zkclientx != null) {
			this.zkclientx.subscribeStateChanges(new IZkStateListener() {
				public void handleStateChanged(KeeperState state) throws Exception {}
				public void handleNewSession() throws Exception {
					initCid(path);
				}

				@Override
				public void handleSessionEstablishmentError(Throwable error) throws Exception {
					LOGGER.error("failed to connect to zookeeper", error);
				}
			});
		}

		if (runningMonitor != null && !runningMonitor.isStart()) {
			runningMonitor.start();
		}
		
		
		//
		// 定时持久化offset
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					// offset 数据持久化
					if ( runningMonitor != null && runningMonitor.isMineRunning() && localAdmin != null )
						localAdmin.saveAll();

				} catch (Exception e) {
					LOGGER.warn("offsetAdmin err: ", e);
				}

			}
		}, 30, 30, TimeUnit.SECONDS);


	}
	
	public void close() {
		
		LOGGER.info("## stop offset ha [{}]", localIp);

		if (runningMonitor != null && runningMonitor.isStart()) {
			runningMonitor.stop();
		}

		
		
		// 释放工作节点
		final String path = zkPathUtil.getClusterHostPath( localIp );
		releaseCid(path);
		
		
		//
		try {
			// 关闭定时任务
			executorService.shutdown();

			// 提交本地剩余offset
			if ( runningMonitor != null && runningMonitor.isMineRunning() && localAdmin != null )
				localAdmin.saveAll();
			
		} catch (Exception e) {
		}
		
	}
	
	///
	

	private void initCid(String path) {
		// 初始化系统目录
		if (zkclientx != null) {
			try {
				zkclientx.createEphemeral(path);	// 临时节点
			} catch (ZkNoNodeException e) {
				// 如果父目录不存在，则创建
				String parentDir = path.substring(0, path.lastIndexOf('/'));
				zkclientx.createPersistent(parentDir, true);
				zkclientx.createEphemeral(path);
			} catch (ZkNodeExistsException e) {
				// ignore
				// 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
			}
		}
	}

	private void releaseCid(String path) {
		
		// 初始化系统目录
		if (zkclientx != null) {
			zkclientx.delete(path);
		}
	}
	
	//-----------------------------------------------------------------------------------------------------
	
	// slave 节点从master上获取 offset
	public long getOffsetForSlave(String user, String topic, int partition) {
		
		// 如果本机不是master,说明网络异常，目前没有master
		if (!runningMonitor.isMineRunning()) {
			return -1;
		}
		
		UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
		if (userCfg == null) {
			return -1;
		}
		
		KafkaPoolCfg poolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get(userCfg.getPoolId());
		if (poolCfg == null) {
			return -1;
		}
		
		TopicCfg topicCfg = poolCfg.getTopicCfgMap().get(topic);
		if (topicCfg == null) {
			return -1;
		}
		
		return localAdmin.getOffset(topicCfg, user, partition);
	}
	
	// 获取offset
	public long getOffset(String user, TopicCfg topicCfg, int partition) {
		long offset;
		if (runningMonitor.isMineRunning()) {
			offset = localAdmin.getOffset(topicCfg, user, partition);
			
		} else {
			ServerRunningData master = this.runningMonitor.getActiveData();
			offset = remoteAdmin.getOffset(master.getAddress(), user, topicCfg.getName(), partition);
		}
		return offset;
	}

	// 回收 offset
	public void rollbackConsumerOffset(String user, String topic, int partition, long offset) {
		if (offset < 0) {
			return;
		}
		if (runningMonitor.isMineRunning()) {
			localAdmin.rollbackConsumerOffset(user, topic, partition, offset);
		} else {
			ServerRunningData master = this.runningMonitor.getActiveData();
			remoteAdmin.rollbackConsumerOffset(master.getAddress(), user, topic, partition, offset);
		}
	}
	
	// 回滚slave节点上的offset
	public void rollbackConsumerOffsetForSlave(String user, String topic, int partition, long offset) {
		if (runningMonitor.isMineRunning()) {
			localAdmin.rollbackConsumerOffset(user, topic, partition, offset);
		}
	}

	// 更新生产offset
	public void updateProducerOffset(String user, String topic, int partition, long offset, long logStartOffset) {
		// TODO 因为影响很小，所以为了减少slave master之间的调用，对slave节点不更新生产点位
		if (runningMonitor.isMineRunning()) {
			localAdmin.updateProducerOffset(user, topic, partition, offset, logStartOffset);
		}
	}
}
