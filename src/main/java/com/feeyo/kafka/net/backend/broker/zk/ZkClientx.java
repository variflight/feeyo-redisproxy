package com.feeyo.kafka.net.backend.broker.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;

/**
 * 使用自定义的ZooKeeperx for zk connection
 */
public class ZkClientx extends ZkClient {

    // 对于zkclient进行一次缓存，避免一个jvm内部使用多个zk connection
	private static Map<String, ZkClientx> clients = new ConcurrentHashMap<String, ZkClientx>();

	private static Object _lock = new Object();

	public static ZkClientx getZkClient(String servers) {

		ZkClientx zkClientx = clients.get(servers);
		if (zkClientx == null) {
			synchronized (_lock) {
				if (zkClientx == null) {
					zkClientx = new ZkClientx(servers);
					clients.put(servers, zkClientx);
				}
			}
		}
		return zkClientx;
	}

    public ZkClientx(String serverstring){
        this(serverstring, Integer.MAX_VALUE);
    }

    public ZkClientx(String zkServers, int connectionTimeout){
        this(new ZkConnection(zkServers), connectionTimeout);
    }

    public ZkClientx(String zkServers, int sessionTimeout, int connectionTimeout){
        this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout);
    }

    public ZkClientx(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer){
        this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
    }

    private ZkClientx(IZkConnection connection, int connectionTimeout){
        this(connection, connectionTimeout, new ByteSerializer());
    }

    private ZkClientx(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer){
        super(zkConnection, connectionTimeout, zkSerializer);
    }

    /**
     * 创建一个持久的顺序节点
     */
	public String createPersistentSequential(String path, boolean createParents)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        try {
            return create(path, null, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            return createPersistentSequential(path, createParents);
        }
    }

	public String createPersistentSequential(String path, Object data, boolean createParents)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
        try {
            return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            return createPersistentSequential(path, data, createParents);
        }
    }

    /**
     * 创建一个持久的节点
     */
    public void createPersistent(String path, Object data, boolean createParents)
    		throws ZkInterruptedException,  IllegalArgumentException, ZkException, RuntimeException {
        try {
            create(path, data, CreateMode.PERSISTENT);
        } catch (ZkNodeExistsException e) {
            if (!createParents) {
                throw e;
            }
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            createPersistent(path, data, createParents);
        }
    }
}
