package com.feeyo.kafka.net.backend.broker.zk.running;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.net.backend.broker.zk.ZkClientx;
import com.feeyo.kafka.net.backend.broker.zk.util.BooleanMutex;
import com.feeyo.kafka.util.JsonUtils;

/**
 * 针对server的running节点控制
 */
public class ServerRunningMonitor {

    private static final Logger        LOGGER       = LoggerFactory.getLogger(ServerRunningMonitor.class);
    private ZkClientx                  zkClient;
    private String                     path;		
    private IZkDataListener            dataListener;
    private BooleanMutex               mutex        = new BooleanMutex(false);
    private volatile boolean           release      = false;
    
    private volatile boolean running = false; // 是否处于运行中
    
    // 当前服务节点状态信息
    private ServerRunningData          serverData;
    
    // 当前实际运行的节点状态信息
    private volatile ServerRunningData activeData;
    private ScheduledExecutorService   delayExector = Executors.newScheduledThreadPool(1);
    private int                        delayTime    = 5;
    private ServerRunningListener      listener;

    public ServerRunningMonitor(ServerRunningData serverData){
        this();
        this.serverData = serverData;
    }

    public ServerRunningMonitor(){
        // 创建父节点
        dataListener = new IZkDataListener() {

            public void handleDataChange(String dataPath, Object data) throws Exception {
                ServerRunningData runningData = JsonUtils.unmarshalFromByte((byte[]) data, ServerRunningData.class);
                if (!isMine(runningData.getAddress())) {
                    mutex.set(false);
                }

                if (!runningData.isActive() && isMine(runningData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
                    release = true;
                    releaseRunning();// 彻底释放mainstem
                }

                activeData = (ServerRunningData) runningData;
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                mutex.set(false);
                if (!release && activeData != null && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    delayExector.schedule(new Runnable() {

                        public void run() {
                            initRunning();
                        }
                    }, delayTime, TimeUnit.SECONDS);
                }
            }
        };

    }

    public void init() {
        processStart();
    }

    public boolean isStart() {
        return running;
    }
    
	public void start() {

		if (running) {
			return; // throw error...
		}

		running = true;

		processStart();
		if (zkClient != null) {
			// 如果需要尽可能释放 channel资源，不需要监听running节点，不然即使stop了这台机器，另一台机器立马会start
			zkClient.subscribeDataChanges(path, dataListener);

			initRunning();
		} else {
			processActiveEnter();// 没有zk，直接启动
		}
	}

    public void release() {
        if (zkClient != null) {
            releaseRunning(); // 尝试一下release
        } else {
            processActiveExit(); // 没有zk，直接启动
        }
    }

    public void stop() {
       
		if (!running) {
			return; //
		}

		running = false;

        if (zkClient != null) {
            zkClient.unsubscribeDataChanges(path, dataListener);

            releaseRunning(); // 尝试一下release
        } else {
            processActiveExit(); // 没有zk，直接启动
        }
        processStop();
    }

    private void initRunning() {
        if (!isStart()) {
            return;
        }

        // 序列化
        byte[] bytes = JsonUtils.marshalToByte(serverData);
        try {
            mutex.set(false);
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);	// 
            activeData = serverData;
            processActiveEnter();// 触发一下事件
            mutex.set(true);
        } catch (ZkNodeExistsException e) {
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                activeData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            }
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(path, true); // 尝试创建父节点
            initRunning();
        }
    }

    /**
     * 阻塞等待自己成为active，如果自己成为active，立马返回
     * 
     * @throws InterruptedException
     */
    public void waitForActive() throws InterruptedException {
        initRunning();
        mutex.get();
    }

    /**
     * 检查当前的状态
     */
    public boolean check() {
        try {
            byte[] bytes = zkClient.readData(path);
            ServerRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            activeData = eventData;// 更新下为最新值
            // 检查下nid是否为自己
            boolean result = isMine(activeData.getAddress());
            if (!result) {
                LOGGER.warn("kafka cmd is running in node[{}] , but not in node[{}]",
                    activeData.getAddress(),
                    serverData.getAddress());
            }
            return result;
        } catch (ZkNoNodeException e) {
            LOGGER.warn("kafka cmd is not run any in node");
            return false;
        } catch (ZkInterruptedException e) {
            LOGGER.warn("kafka cmd check is interrupt");
            Thread.interrupted();// 清除interrupt标记
            return check();
        } catch (ZkException e) {
            LOGGER.warn("kafka cmd check is failed");
            return false;
        }
    }

    private boolean releaseRunning() {
        if (check()) {
            zkClient.delete(path);
            mutex.set(false);
            processActiveExit();
            return true;
        }

        return false;
    }
    
    //
    private boolean isMine(String address) {
        return address.equals(serverData.getAddress());
    }

    private void processStart() {
        if (listener != null) {
            try {
                listener.processStart();
            } catch (Exception e) {
                LOGGER.error("processStart failed", e);
            }
        }
    }

    private void processStop() {
        if (listener != null) {
            try {
                listener.processStop();
            } catch (Exception e) {
                LOGGER.error("processStop failed", e);
            }
        }
    }

    private void processActiveEnter() {
        if (listener != null) {
            try {
                listener.processActiveEnter();
            } catch (Exception e) {
                LOGGER.error("processActiveEnter failed", e);
            }
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            try {
                listener.processActiveExit();
            } catch (Exception e) {
                LOGGER.error("processActiveExit failed", e);
            }
        }
    }
    
    public void setPath(String path) {
		this.path = path;
	}

	public void setListener(ServerRunningListener listener) {
        this.listener = listener;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public void setServerData(ServerRunningData serverData) {
        this.serverData = serverData;
    }

    public void setZkClient(ZkClientx zkClient) {
        this.zkClient = zkClient;
    }

	public ServerRunningData getActiveData() {
		return activeData;
	}
	
	public boolean isMineRunning() {
		return mutex.state();
	}

}