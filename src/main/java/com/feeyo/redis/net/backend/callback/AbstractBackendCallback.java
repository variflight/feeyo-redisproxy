package com.feeyo.redis.net.backend.callback;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

/**
 * 支持 Todo Task
 * 
 * @author zhuam
 *
 */
public abstract class AbstractBackendCallback implements BackendCallback {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractBackendCallback.class );
	
	private ArrayList<TodoTask> todoTasks = new ArrayList<TodoTask>(2);
	  
	public void addTodoTask(TodoTask task) {
		this.todoTasks.add(task);
	}
	
	// 获取前端连接
	protected RedisFrontConnection getFrontCon(RedisBackendConnection backendCon) {
		return (RedisFrontConnection) backendCon.getAttachement();
	}

	// 后端连接异常 & 错误
	// ===================================================
	@Override
	public void connectionAcquired(RedisBackendConnection backendCon) {
		
		// 执行挂起的任务
		if (this.todoTasks.isEmpty()) {
			return;
		}
		
		try {
			TodoTask task = this.todoTasks.remove(0);
			if (task != null) {
				task.execute( backendCon );
			}
		} catch (Exception e) {
			LOGGER.error("exec pending task err:" + this, e);
			
			RedisFrontConnection frontCon = getFrontCon(backendCon);
			frontCon.close( e.toString() );
		}
	}

	@Override
	public void connectionError(Exception e, RedisBackendConnection backendCon) {
		RedisFrontConnection frontCon = getFrontCon(backendCon);
		if (frontCon != null)
			frontCon.getSession().backendConnectionError(e);
	}

	@Override
	public void connectionClose(RedisBackendConnection backendCon, String reason) {

		RedisFrontConnection frontCon = getFrontCon(backendCon);
		if (frontCon != null)
			frontCon.getSession().backendConnectionClose(reason);

		// 后端连接关闭, 清理连接池内的 connection
		backendCon.getPhysicalNode().removeConnection(backendCon);
	}

}
