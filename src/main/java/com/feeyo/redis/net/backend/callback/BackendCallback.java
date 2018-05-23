package com.feeyo.redis.net.backend.callback;

import java.io.IOException;

import com.feeyo.redis.net.backend.BackendConnection;

/**
 * 后端数据库的事件处理回调接口
 * 
 * @author wuzhihui
 * @author zhuam
 *
 */
public interface BackendCallback  {
	
    /**
     * 已获得有效连接的响应处理
     */
    void connectionAcquired(BackendConnection conn);

    /**
     * 无法获取连接
     */
    void connectionError(Exception e, BackendConnection conn);


    /**
     * 收到数据包的响应处理
     */
   void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException;

    /**
     * 连接关闭
     */
   void connectionClose(BackendConnection conn, String reason);
    
}