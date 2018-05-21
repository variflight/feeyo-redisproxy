package com.feeyo.kafka.net.backend.broker.offset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.OffsetManageCfg;
import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.net.backend.broker.zk.ZkClientx;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningData;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningListener;
import com.feeyo.kafka.net.backend.broker.zk.running.ServerRunningMonitor;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.util.NetworkUtil;


/**
 * 
 * <pre>
 *  存储结构：
 *  
 * /redis-proxy
 *      	cluster
 *      		host1  (ip+port)
 *      		host2
 *      	running (EPHEMERAL) 
 * 
 * 触发HA自动切换的场景
 * 
 * 1. 正常场景
 * 	   a. 正常关闭 server(会释放channel的所有资源，包括删除running节点)
 * 	   b. 平滑切换(gracefully)
 * 		操作：更新对应destination的running节点内容，将"active"设置为false，对应的running节点收到消息后，会主动释放running节点，让出控制权但自己jvm不退出，gracefully.
 *			 {"active":false,"address":"127.0.0.1:11111","cid":1}
 * 
 * 2. 异常场景
 * 	   a.  server对应的jvm异常crash，running节点的释放会在对应的zookeeper session失效后，释放running节点(EPHEMERAL节点)
 * 		 ps. session过期时间默认为zookeeper配置文件中定义的tickTime的20倍，如果不改动zookeeper配置，那默认就是40秒
 * 
 * 	   b.  server所在的网络出现闪断，导致zookeeper认为session失效，释放了running节点，此时 server对应的jvm并未退出，(一种假死状态，非常特殊的情况)
 * 		 ps. 为了保护假死状态的 server，避免因瞬间running失效导致channel重新分布，
 * 			所以做了一个策略： server在收到running节点释放后，延迟一段时间抢占running，
 * 			原本running节点的拥有者可以不需要等待延迟，优先取得running节点，可以保证假死状态下尽可能不无谓的释放资源。 
 * 			目前延迟时间的默认值为5秒，即running节点针对假死状态的保护期为5秒.
 * 
 * </pre>
 * 
 * 
 * @author zhuam
 *
 */
public class RunningOffsetZkService {
	
	private static Logger LOGGER = LoggerFactory.getLogger(RunningOffsetZkService.class);
	private static final String ZK_CFG_FILE = "kafka.xml"; // zk settings is in server.xml
	
	private ZkClientx  zkclientx;
	
	private ServerRunningData runningData;
	private ServerRunningMonitor runningMonitor;	// HA 监控
	
	private String address;
	private OffsetManageCfg offsetManageCfg;
	
	private static RunningOffsetZkService INSTANCE;
	private static Object _lock = new Object();
	
	public static RunningOffsetZkService INSTANCE() {
		if (INSTANCE == null) {
			synchronized (_lock) {
				if (INSTANCE == null) {
					try {
						INSTANCE = new RunningOffsetZkService();
					} catch (Exception e) {
						LOGGER.error("", e);
					}
				}
			}
		}
		return INSTANCE;
	}
	
	private RunningOffsetZkService() throws FileNotFoundException, IOException {
		
		offsetManageCfg = KafkaConfigLoader.loadOffsetManageCfg(ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE));
		
		address = NetworkUtil.getLocalAddress();
		runningData = new ServerRunningData(address);
		
		// HA
		zkclientx = ZkClientx.getZkClient(offsetManageCfg.getServer());
		runningMonitor = new ServerRunningMonitor( runningData );
		runningMonitor.setPath(offsetManageCfg.getRunningPath());
		runningMonitor.setListener(new ServerRunningListener() {
			@Override
			public void processStart() {}

			@Override
			public void processStop() {
				RunningOffsetAdmin.INSTANCE().close();
			}

			@Override
			public void processActiveEnter() {
				// start
				try {
					RunningOffsetAdmin.INSTANCE().startup(offsetManageCfg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			@Override
			public void processActiveExit() {
				RunningOffsetAdmin.INSTANCE().close();
			}
        });
        
        if ( zkclientx != null) {
            runningMonitor.setZkClient(zkclientx);
        }
        
        // 触发创建一下cid节点
        runningMonitor.init();
	}
	
	public void start() throws IOException {
		
		LOGGER.info("## start the redis-proxy server[{}]", address);
		
		final String path = offsetManageCfg.getClusterPath() + File.separator + address;
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

	}
	
	public void stop() {

		LOGGER.info("## stop the redis-proxy server[{}]", address);

		if (runningMonitor != null && runningMonitor.isStart()) {
			runningMonitor.stop();
		}

		final String path = offsetManageCfg.getClusterPath() + File.separator + address;
		// 释放工作节点
		releaseCid(path);

	}

	public boolean isMaster() {
		return runningMonitor.isMineRunning();
	}
	
	public ServerRunningData getMasterServerRunningData() {
		return runningMonitor.getActiveData();
	}
	
	//-------------------------------------------------------------------
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

}
