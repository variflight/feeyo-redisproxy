package com.feeyo.redis.config.loader.zk;

import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.ZkCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.util.Log4jInitializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * zookeeper client for redis proxy
 *
 * <p> For the very first connection, download if zk has the configs already otherwise upload the local config file to zk
 *
 * @author Tr!bf wangyamin@variflight.com
 *
 */
public class ZkClient implements ConnectionStateListener {
    private static Logger LOGGER = LoggerFactory.getLogger( ZkClient.class );

    private static final String ZK_CFG_FILE = "server.xml"; // zk settings is in server.xml
    private final static ZkClient instance = new ZkClient();
    private final ConcurrentMap<String/*zk node path*/, ConcurrentMap<ZkDataListener, CuratorWatcher>> acitveDataListeners = new ConcurrentHashMap<>();
    private final Map<String/*local file path*/, ZkDataListener> allDataListeners = new HashMap<>();

    private CuratorFramework curator;
    private AtomicBoolean zkConnected = new AtomicBoolean(false);
    private ZkCfg zkCfg;

    private ZkClient() {

    }

    public static ZkClient INSTANCE() {

        return instance;
    }

    public void init() {
        ZkCfg zkCfg = ConfigLoader.loadZkCfg( ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE) );
        init(zkCfg);
    }

    private void init(ZkCfg zkCfg) {
        this.zkCfg = zkCfg;

        if (zkCfg==null || !zkCfg.isUsingZk())
            return;

        for (String cfgName : zkCfg.getLocCfgNames()) {
            String locFilePath = ConfigLoader.buidCfgAbsPathFor(cfgName);
            String backupPath = zkCfg.buildCfgBackupPathFor(cfgName);
            ConfigFileListener configFileListener = new ConfigFileListener(locFilePath, backupPath, zkCfg.isAutoAct());
            allDataListeners.put(locFilePath, configFileListener);
        }

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkCfg.buildConnectionString())
                .retryPolicy(new RetryNTimes(3, 1000))
                .connectionTimeoutMs(3000);

        curator = builder.build();

        curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState state) {
                ZkClient.this.stateChanged(client,state);
            }
        });
        curator.start();

        // 阻塞直到配置文件下载完成 或者 ZK 连接错误 或者 超时
        try {
            synchronized (this) {
                this.wait(30_000);
            }

            LOGGER.info("init done, is connected: " + zkConnected.get());
            if (!zkConnected.get())
                this.close();

        } catch (InterruptedException e) {
        }
    }

    public void reloadZkCfg() {
        ZkCfg newZkCfg = ConfigLoader.loadZkCfg( ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE) );
        if ( newZkCfg != null && !newZkCfg.equals(this.zkCfg) ) {
            this.close();
            this.init(newZkCfg);
        }
    }

    public boolean isOnline() {
        return zkConnected.get();
    }

    public void createZkNode(String path, boolean ephemeral) {
        try {
            if (curator.checkExists().forPath(path) != null)
                return;
        } catch (Exception e) {
            LOGGER.warn("",e);
        }

        int i = path.lastIndexOf('/');
        if (i > 0) {
            createZkNode(path.substring(0,i), false);
        }

        if (ephemeral) {
            createEphemeral(path);
        } else {
            createPersistent(path);
        }
    }

    protected void createPersistent(String path) {
        try {
            curator.create().forPath(path);
        } catch (Exception e) {
            LOGGER.warn("",e);
        }
    }

    protected void createEphemeral(String path) {
        try {
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        } catch (Exception e) {
            LOGGER.warn("",e);
        }
    }

    public void addDataListener (String path, ZkDataListener dataListener) {
        if (dataListener == null)
            return;

        ConcurrentMap<ZkDataListener, CuratorWatcher> listeners = acitveDataListeners.get(path);
        if (listeners == null) {
            acitveDataListeners.putIfAbsent(path,new ConcurrentHashMap<ZkDataListener, CuratorWatcher>());
            listeners = acitveDataListeners.get(path);
        }

        CuratorWatcher curatorWatcher = listeners.get(dataListener);
        if (curatorWatcher == null) {
            listeners.putIfAbsent(dataListener,new CuratorWatcherImpl(dataListener));
            curatorWatcher = listeners.get(dataListener);
        }

        try {
            curator.getData().usingWatcher(curatorWatcher).forPath(path);
        } catch (Exception e) {
            LOGGER.warn("",e);
        }
    }

    public void removeDataListener(String path, ZkDataListener listener) {
        if (listener == null)
            return;

        ConcurrentMap<ZkDataListener, CuratorWatcher> listeners = acitveDataListeners.get(path);
        if (listeners != null) {
            CuratorWatcher curatorWatcher = listeners.remove(listener);
            if (curatorWatcher != null) {
                ((CuratorWatcherImpl)curatorWatcher).unwatch();
            }
        }
    }

    public void setZkNodeData(String zkNodePath, byte[] data) throws Exception {
        Stat stat = curator.checkExists().forPath(zkNodePath);
        if (stat == null) {
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), zkNodePath);
        }

        curator.setData().inBackground().forPath(zkNodePath, data);
    }

    public void close() {
        LOGGER.warn("close the ZkClient");

        zkConnected.set(false);
        acitveDataListeners.clear();
        allDataListeners.clear();

        if (curator != null) {
            CloseableUtils.closeQuietly(curator);
            curator = null;
        }
    }

    public static void saveAndBackupCfgData(byte[] data, String locFilePath, String backupPath) {
        BufferedWriter bw = null;
        FileWriter fw = null;

        try {
            File tmp = new File(locFilePath+".tmp");
            fw = new FileWriter(tmp);
            bw = new BufferedWriter(fw);
            bw.write(new String(data));
            bw.flush();
            bw.close();
            bw = null;
            fw.close();
            fw = null;

            // backup the original configure file
            File targetFile = new File(locFilePath);
            File backupFile = new File(backupPath);
            if (!backupFile.getParentFile().exists()) {
                if (!backupFile.getParentFile().mkdirs()) {
                    LOGGER.info("failed to mkdir: " + backupFile.getParent());
                }
            }

            if (backupFile.exists()) {
                if (!backupFile.delete()) {
                    LOGGER.info("failed to delete: " + backupPath);
                }
            }

            try {
                Files.move(targetFile.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                Files.move(tmp.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                LOGGER.info("failed to move file: ",e);
            }

        } catch (IOException e) {
            LOGGER.warn("",e);
        } finally {
            try {
                if (bw != null) {
                    bw.flush();
                    bw.close();
                }

                if (fw != null) {
                    fw.close();
                }
            } catch (IOException e) {
                LOGGER.warn("",e);
            }
        }
    }

    public boolean resetAutoActivation(Boolean auto) {
        zkCfg.setAutoAct(auto);
        for (Map.Entry<String, ZkDataListener> entry : allDataListeners.entrySet()) {
            ((ConfigFileListener)entry.getValue()).setAutoActivation(auto);
        }
        return true;
    }

    public boolean upload2zk(String fileName) {
        for (String cfgName : zkCfg.getLocCfgNames()) {
            if (cfgName.equals(fileName)) {
                String locFilePath = ConfigLoader.buidCfgAbsPathFor(cfgName);
                upload2zk(locFilePath, zkCfg.buildZkPathFor(cfgName));
                return true;
            }
        }
        return false;
    }

    private void upload2zk(String locFilePath, String zkNodePath) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(locFilePath), Charset.forName("utf-8")));
            StringBuilder sb = new StringBuilder();

            String line = null;
            while ((line=br.readLine()) != null) {
                sb.append(line).append('\n');
            }

            this.removeDataListener(zkNodePath, allDataListeners.get(locFilePath));
            this.setZkNodeData(zkNodePath,sb.toString().getBytes());
            this.addDataListener(zkNodePath, allDataListeners.get(locFilePath));

        } catch (IOException e) {
            LOGGER.warn("",e);
        } catch (Exception e) {
            LOGGER.warn("",e);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                LOGGER.warn("",e);
            }
        }
    }

    public void createZkInstanceIdByIpPort(String ipAndPort) {
        if (zkConnected.get()) {
            zkCfg.setInstanceIdPath(ipAndPort);
            createZkNode(zkCfg.getInstanceIdPath(),true);
            LOGGER.info("create redis-proxy instance id: {}", new Object[] {zkCfg.getInstanceIdPath()});
        } else {
            LOGGER.info("ZkClient is offline");
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState state) {
        switch (state) {
            case CONNECTED:
                LOGGER.info("connected to zookeeper");
                try {
                    for (String cfgName : zkCfg.getLocCfgNames()) {
                        String locFilePath = ConfigLoader.buidCfgAbsPathFor(cfgName);
                        String zkNodePath = zkCfg.buildZkPathFor(cfgName);
                        String backupPath = zkCfg.buildCfgBackupPathFor(cfgName);

                        if (curator.checkExists().forPath(zkNodePath) != null) {
                            LOGGER.info("download configure file: {} to {}", new Object[] {zkNodePath, locFilePath});
                            byte[] data = curator.getData().forPath(zkNodePath);
                            saveAndBackupCfgData(data, locFilePath, backupPath);

                            if (zkCfg.isAutoAct()) {
                                switch (cfgName) {
                                    case "user.xml":
                                        RedisEngineCtx.INSTANCE().reloadUser();
                                        break;
                                    case "pool.xml":
                                        RedisEngineCtx.INSTANCE().reloadPool();
                                        break;
                                    case "server.xml":
                                        RedisEngineCtx.INSTANCE().reloadServer();
                                        break;
                                    default:
                                        break;
                                }
                            }
                        } else {
                            LOGGER.info("zookeeper is not initialized. upload configure file: {} to {}", new Object[] {locFilePath, zkNodePath});
                            upload2zk(locFilePath, zkNodePath);
                        }

                        this.addDataListener(zkNodePath, allDataListeners.get(locFilePath));
                    }

                    synchronized (this) {
                        zkConnected.set(true);
                        if (zkCfg.getInstanceIdPath() != null) {
                            createZkNode(zkCfg.getInstanceIdPath(),true);
                        }
                        this.notifyAll();
                    }
                } catch (Exception e) {
                    LOGGER.warn("",e);
                }

                break;
            case LOST:
                synchronized (this) {
                    zkConnected.set(false);
                    this.notifyAll();
                }
                this.close();
                break;
            case RECONNECTED:

                break;

            default:
                break;
        }
    }

    private class CuratorWatcherImpl implements CuratorWatcher {

        private volatile ZkDataListener listener;

        public CuratorWatcherImpl(ZkDataListener listener) {
            this.listener = listener;
        }

        public void unwatch() {
            this.listener = null;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                if (listener != null) {
                    listener.zkDataChanged(curator.getData().usingWatcher(this).forPath(event.getPath()));
                }
            }
        }
    }

    public static void main(String[] args) {
        if ( System.getProperty("FEEYO_HOME") == null ) {
            System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
        }
        Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

        ZkCfg zkCfg = ConfigLoader.loadZkCfg(ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE));
        System.out.println(zkCfg);

        ZkClient.INSTANCE().init();

        try {
            while(true) {
                Thread.sleep(100);
                if (ZkClient.INSTANCE().isOnline()) {
                    ZkClient.INSTANCE().upload2zk(ConfigLoader.buidCfgAbsPathFor("pool.xml"), zkCfg.buildZkPathFor("pool.xml"));
                    break;
                }
            }
        } catch (InterruptedException e) {
        }
    }
}
