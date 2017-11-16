package com.feeyo.redis.engine.manage.stat;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisResponseDecoderV4;
import com.feeyo.redis.engine.codec.RedisResponseV3;
import com.feeyo.redis.engine.manage.stat.StatUtil.CollectionKey;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.RedisFrontSession;
import com.feeyo.redis.net.front.handler.CommandParse;

public class CollectionKeysHandler {
	private static Logger LOGGER = LoggerFactory.getLogger( CollectionKeysHandler.class );
	
	// 取length命令
	private static final String HLEN_COMMAND = "*2\r\n$4\r\nHLEN\r\n";
	private final static String LLEN_COMMAND = "*2\r\n$4\r\nLLEN\r\n"; 
	private final static String SCARD_COMMAND = "*2\r\n$5\r\nSCARD\r\n";
	private final static String ZCARD_COMMAND  = "*2\r\n$5\r\nZCARD\r\n"; 
	
	private static final String HLEN = "HLEN";
	private final static String LLEN = "LLEN"; 
	private final static String SCARD = "SCARD";
	private final static String ZCARD = "ZCARD"; 
	private final static int COLLECTION_KEY_LENGTH_THRESHOLD = 10000;
	
	public void Handle(ConcurrentHashMap<String, CollectionKey> collectionKeys) {
		for (Entry<String, CollectionKey> entry : collectionKeys.entrySet()) {
			CollectionKey collectionKey = entry.getValue();
			String user = collectionKey.user;
			UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
			
			if (userCfg != null) {
				PhysicalNode physicalNode = null;
				AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( userCfg.getPoolId() );
				// 单节点
				if (pool.getType() == 0) {
					physicalNode = pool.getPhysicalNode();
					
				// 集群池
				} else if (pool.getType() == 1) {
					RedisClusterPool clusterPool = (RedisClusterPool) pool;
					// 计算key的slot值。
					int slot = ClusterCRC16Util.getSlot(collectionKey.key);
					// 根据 slot 获取 redis物理节点
					physicalNode = clusterPool.getPhysicalNodeBySlot(slot);
				}
				RedisFrontConnection frontCon = new RedisFrontConnection(null);
				RedisFrontSession session = frontCon.getSession();
				final byte[] buffer = getRequestCommand(collectionKey, session);
				session.setRequestKey( collectionKey.key.getBytes() );
				AbstractBackendCallback callback = new CollectionKeyCallback();
				RedisBackendConnection backendCon = null;
				try {
					backendCon = physicalNode.getConnection(callback, frontCon);
					if ( backendCon == null ) {
						// 连接建立成功后需要处理的任务
						TodoTask task = new TodoTask() {				
							@Override
							public void execute(RedisBackendConnection backendCon) throws Exception {	
								backendCon.write( buffer );
							}
						};
						callback.addTodoTask(task);
						// 创建新连接
						backendCon = physicalNode.createNewConnection(callback, frontCon);
					} else {
						backendCon.write(buffer);
					}
				} catch (Exception e) {
					if (backendCon != null)
						backendCon.close(e.getMessage());
					LOGGER.error("", e);
				}
			}
			
		}
	}
	
	private byte[] getRequestCommand(CollectionKey collectionKey, RedisFrontSession session) {
		StringBuffer sb = new StringBuffer();
		String key  = collectionKey.key;
		byte type = collectionKey.type;
		if (type == CommandParse.TYPE_HASH_CMD) {
			sb.append(HLEN_COMMAND);
			session.setRequestCmd(HLEN);
		} else if (type == CommandParse.TYPE_LIST_CMD) {
			sb.append(LLEN_COMMAND);
			session.setRequestCmd(LLEN);
		} else if (type == CommandParse.TYPE_SET_CMD) {
			sb.append(SCARD_COMMAND);
			session.setRequestCmd(SCARD);
		} else if (type == CommandParse.TYPE_SORTEDSET_CMD) {
			sb.append(ZCARD_COMMAND);
			session.setRequestCmd(ZCARD);
		}
		sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
		
		return sb.toString().getBytes();
	}
	
	
	private class CollectionKeyCallback extends AbstractBackendCallback {
		private RedisResponseDecoderV4 decoder = new RedisResponseDecoderV4();
		@Override
		public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {
			List<RedisResponseV3> resps = decoder.decode( byteBuff );
			
			if (resps!= null && resps.size() == 1) {
				RedisResponseV3 response = resps.get(0);
				if (response.type() == ':') {
					int len = getIntValue((byte[]) response.data());
					
					RedisFrontConnection frontCon = getFrontCon( backendCon );
					RedisFrontSession session = frontCon.getSession();
					CollectionKey collectionKey = new CollectionKey();
					collectionKey.key = new String(session.getRequestKey());
					collectionKey.type = CommandParse.getPolicy(session.getRequestCmd()).getType();
					
					if (len > COLLECTION_KEY_LENGTH_THRESHOLD) {
						collectionKey.length.set(len);
						StatUtil.addCollectionKeyToTop100(collectionKey);
					} else {
						StatUtil.removeCollectionKeyFromTop100(collectionKey);
					}
				}
				// 后段链接释放
				backendCon.release();	
			}
		}
	}
	
	// 读取长度
	private int getIntValue(byte[] data) {
		int result = 0;
		boolean isNeg = false;	
		if (data == null || data.length < 1) {
			return -1;
		}
		int offset = 1;
		byte b = data[offset];
		if ( b == '-' )  {
			isNeg = true;			
			offset++;
			if ( offset >= data.length ) {
				return -1;
			}		
			b = data[ offset ];
		}
		while (b != '\r') {
			int value = b - '0';
			if (value >= 0 && value < 10) {
				result *= 10;
				result += value;
			} else {
				return 0;
			}
			offset++;
			if (offset >= data.length) {
				return 0;
			}
			b = data[offset];
		}
		result = (isNeg ? -result : result);
		return result;
	}
	
}
