package com.feeyo.redis.net.backend.pool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.net.backend.BackendConnection;

/**
 *  抽象连接池
 * 
 * @author zhuam
 *
 */
public abstract class AbstractPool {
	
	protected Logger LOGGER = LoggerFactory.getLogger( AbstractPool.class );

	protected static final byte[] PING = "*1\r\n$4\r\nPING\r\n".getBytes();
	
	// 有效性检测标记
	protected AtomicBoolean availableCheckFlag = new AtomicBoolean( false );
	
	protected PoolCfg poolCfg;
	
	public AbstractPool(final PoolCfg poolCfg) {		
		this.poolCfg = poolCfg;				
	}
	
	public int getId() {
		return poolCfg.getId();
	}
	
	public String getName() {
		return poolCfg.getName();
	}

	public int getType() {
		return poolCfg.getType();
	}
	
	public List<String> getNodes() {
		return poolCfg.getNodes();
	}

	public abstract boolean startup();	
	public abstract boolean close(boolean isForce);
	
	public abstract PhysicalNode getPhysicalNode();		
	public abstract PhysicalNode getPhysicalNode(String cmd, String key);
	public abstract PhysicalNode getPhysicalNode(String cmd, byte[] key);
	public abstract PhysicalNode getPhysicalNode(int id);
	
	/**
	 * 测试连通性
	 */
	public abstract boolean testConnection();
	
	/**
	 * 可用性检查 
	 */
	public abstract void availableCheck();
	
	/**
	 * 心跳检查
	 */
	public abstract void heartbeatCheck(long timeout);

	/**
	 * 延迟时间统计
	 */
	public abstract void latencyTimeCheck(long epoch);


	//TODO: 此处几个方法待进一步优化
	//-------------------------------------------------
	protected LinkedList<BackendConnection> getNeedHeartbeatCons(
			ConcurrentLinkedQueue<BackendConnection> checkLis, long heartbeatTime, long closeTime) {
		
		int maxConsInOneCheck = 10;
		LinkedList<BackendConnection> heartbeatCons = new LinkedList<BackendConnection>();
		
		Iterator<BackendConnection> checkListItor = checkLis.iterator();
		while (checkListItor.hasNext()) {
			BackendConnection con = checkListItor.next();
			if ( con.isClosed() ) {
				checkListItor.remove();
				continue;
			}
			
			// 关闭 闲置过久的 connection
			if (con.getLastTime() < closeTime) {
				if(checkLis.remove(con)) { 
					con.close("heartbeate idle close ");
					continue;
				}
			}
			
			// 提取需要做心跳检测的 connection
			if (con.getLastTime() < heartbeatTime && heartbeatCons.size() < maxConsInOneCheck) {
				// 如果移除失败，说明该连接已经被其他线程使用
				if(checkLis.remove(con)) { 
					con.setBorrowed(true);
					heartbeatCons.add(con);
				}
			} 
		}
		
		return heartbeatCons;
	}
	
	protected void closeByIdleMany(PhysicalNode physicalNode, int ildeCloseCount) {	
		
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("too many ilde cons, close some for pool  " + this.getName() );
		
		List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>( ildeCloseCount);
		readyCloseCons.addAll( physicalNode.conQueue.getIdleConsToClose(ildeCloseCount));

		for (BackendConnection idleCon : readyCloseCons) {
			if ( idleCon.isBorrowed() ) {
				LOGGER.warn("find idle con is using " + idleCon);
			}
			idleCon.close("too many idle con");
		}
	}
	
	protected void createByIdleLitte(PhysicalNode physicalNode, int idleCons, int createCount) {
		
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("create connections, because idle connection not enough ,cur is "
					+ idleCons
					+ ", minCon is "
					+  poolCfg.getMinCon()
					+ " for "
					+ this.getName());
		
		for (int i = 0; i < createCount; i++) {			
			int activeCount = physicalNode.getActiveCount();
			int idleCount = physicalNode.getIdleCount();
			
			if ( activeCount + idleCount >= poolCfg.getMaxCon() ) {
				break;
			}			
			try {				
				// create new connection
				physicalNode.createNewConnection();				
			} catch (IOException e) {
				LOGGER.warn("create connection err ", e);
			}
		}
	}	
	
}