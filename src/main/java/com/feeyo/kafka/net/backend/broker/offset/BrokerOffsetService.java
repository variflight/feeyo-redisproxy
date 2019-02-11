package com.feeyo.kafka.net.backend.broker.offset;

import com.feeyo.config.UserCfg;
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
import com.feeyo.redis.engine.RedisEngineCtx;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

//
public class BrokerOffsetService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BrokerOffsetService.class );
	
	//
	private static AtomicBoolean running = new AtomicBoolean( false );
	
	//
	private final OffsetRemoteAdmin remoteAdmin;
	private final OffsetLocalAdmin localAdmin;
	
	// zk ha
	//
	private ZkClientx  zkclientx;
	private ZkPathUtil zkPathUtil;
	private ServerRunningData runningData;
	private ServerRunningMonitor runningMonitor;	// HA 监控
	
	private String zkServerIp = null;
	private String path = null;
	private String localIp = null;
	
	// flush to zk
	//
	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	//
	private static BrokerOffsetService INSTANCE = new BrokerOffsetService();

	public static BrokerOffsetService INSTANCE() {
		return INSTANCE;
	}
	
	//
	private BrokerOffsetService() {
		
		OffsetCfg offsetCfg = KafkaConfigLoader.loadOffsetCfg(ConfigLoader.buidCfgAbsPathFor( "kafka.xml" ));
		this.zkServerIp = offsetCfg.getZkServerIp();
		this.path = offsetCfg.getPath();
		this.localIp = offsetCfg.getLocalIp();
		
		//
		this.localAdmin = new OffsetLocalAdmin( zkServerIp, path );
		this.remoteAdmin = new OffsetRemoteAdmin();
		
		//
		this.zkPathUtil = new ZkPathUtil( path );
		this.zkclientx = ZkClientx.getZkClient( zkServerIp );
	
		// select master
		this.runningData = new ServerRunningData( localIp );
		this.runningMonitor = new ServerRunningMonitor( runningData );
		this.runningMonitor.setPath( zkPathUtil.getMasterRunningPath() );
		this.runningMonitor.setListener(new ServerRunningListener() {
	
			@Override
			public void processActiveEnter() {
				// start
				//
				LOGGER.info("###### start master=" + localIp);
				if ( localAdmin != null)
					localAdmin.init();
			
			}

			@Override
			public void processActiveExit() {
				// stop
				//
				LOGGER.info("###### stop master=" + localIp);
				
			}
        });
        
		runningMonitor.setZkClient(zkclientx);
	}
	
	
	public boolean isRunning() {
		return running.get();
	}
	
	public void start() throws IOException {
		
		//
		if ( !running.compareAndSet(false, true) ) {
			return;
		}
		
		//
		String localIpPath = zkPathUtil.getClusterHostPath( localIp );
		initialize(  localIpPath  );
		
		 // 创建所有工作节点
		this.zkclientx.subscribeStateChanges(new IZkStateListener() {
			public void handleStateChanged(KeeperState state) throws Exception {}
			public void handleNewSession() throws Exception {
				initialize( path );
			}

			@Override
			public void handleSessionEstablishmentError(Throwable error) throws Exception {
				LOGGER.error("failed to connect to zookeeper", error);
			}
		});

		if (runningMonitor != null && !runningMonitor.isStart()) {
			runningMonitor.start();
		}
		
		
		// flush
		// 
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					if ( runningMonitor != null && runningMonitor.isMineRunning() && localAdmin != null ) {
						localAdmin.flushAll();
					}

				} catch (Exception e) {
					LOGGER.warn("offset flush err: ", e);
				}
			}
		}, 30, 30, TimeUnit.SECONDS);

	}
	
	public void stop() {
		
		running.set( false );
		
		// stop running
		if (runningMonitor != null && runningMonitor.isStart()) {
			runningMonitor.stop();
		}

		// release node
		release(  zkPathUtil.getClusterHostPath( localIp ) );
		
		// flush 
		try {	
			if ( executorService != null )
				executorService.shutdown();

			if ( runningMonitor != null && runningMonitor.isMineRunning() && localAdmin != null )
				localAdmin.flushAll();
			
		} catch (Exception e) {
			// ignore
		}
		
		//
		if (zkclientx != null) {
            zkclientx.close();
        }
	}
	
	//
	// -----------------------------------------------------------------------------
	//
	
	//################### cluster path ##################
	// 初始化
	//
	private void initialize(String path) {
		LOGGER.info("## init the path = {}",  path);
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
				// 因为第一次启动时创建了path,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题
			}
		}
	}

	// 卸载
	//
	private void release(String path) {
		 LOGGER.info("## release the path = {}", path);
		if (zkclientx != null) {
			zkclientx.delete(path);
		}
	}
	
	
	//
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
		
		return localAdmin.getOffset(user, topicCfg, partition);
	}
	
	// 回滚slave节点上的offset
	public void returnOffsetForSlave(String user, String topic, int partition, long offset) {
		if (runningMonitor.isMineRunning()) {
			localAdmin.returnOffset(user, topic, partition, offset);
		}
	}
	
	
	// 修复offset
	public boolean repairOffset(String user, TopicCfg topicCfg, int partition, long offset) {

		if (runningMonitor.isMineRunning()) {
			localAdmin.repairOffset(user, topicCfg, partition, offset);
			return true;
			
		} else {
			ServerRunningData master = this.runningMonitor.getActiveData();
			LOGGER.warn(" Please forward jump to host={} port={} execution", master.getIp(), master.getPort());
			return false;
		}
	}
	
	
	// 获取offset
	public long getOffset(String user, TopicCfg topicCfg, int partition) {
		long offset;
		if (runningMonitor.isMineRunning()) {
			offset = localAdmin.getOffset(user, topicCfg, partition);
			
		} else {
			ServerRunningData master = this.runningMonitor.getActiveData();
			offset = remoteAdmin.getOffset(master.getIp(), master.getPort(), user, topicCfg.getName(), partition);
		}
		return offset;
	}

	// 回收 offset
	public void returnOffset(String user, String topic, int partition, long offset) {
		if (offset < 0) {
			return;
		}
		if (runningMonitor.isMineRunning()) {
			localAdmin.returnOffset(user, topic, partition, offset);
		} else {
			ServerRunningData master = this.runningMonitor.getActiveData();
			remoteAdmin.returnOffset(master.getIp(), master.getPort(), user, topic, partition, offset);
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
