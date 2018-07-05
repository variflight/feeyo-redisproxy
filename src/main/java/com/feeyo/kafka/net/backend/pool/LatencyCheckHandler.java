package com.feeyo.kafka.net.backend.pool;

import com.feeyo.redis.engine.manage.stat.LatencyCollector;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyCheckHandler implements BackendCallback {
    private final long epoch;
    private final String nodeId;
    protected Logger LOGGER = LoggerFactory.getLogger(LatencyCheckHandler.class);
    private long requestMilliseconds;

    public LatencyCheckHandler(String nodId, long epoch) {
        this.nodeId = nodId;
        this.epoch = epoch;
    }

    /**
     * 已获得有效连接的响应处理
     *
     * @param conn
     */
    @Override
    public void connectionAcquired(BackendConnection conn) {
    }

    /**
     * 无法获取连接
     *
     * @param e
     * @param conn
     */
    @Override
    public void connectionError(Exception e, BackendConnection conn) {
    }

    @Override
    public void handleResponse(BackendConnection conn, byte[] byteBuff) {

        // +PONG\r\n
        if (byteBuff.length == 7 && byteBuff[0] == '+' && byteBuff[1] == 'P' && byteBuff[2] == 'O' && byteBuff[3] == 'N' &&
                byteBuff[4] == 'G') {

            long cost = System.currentTimeMillis() - requestMilliseconds;
            LatencyCollector.add(nodeId, epoch, cost);

            conn.release();
        } else {
            conn.close("The unexpected response for latency check is " + new String(byteBuff));
        }
    }

    /**
     * 连接关闭
     *
     * @param conn
     * @param reason
     */
    @Override
    public void connectionClose(BackendConnection conn, String reason) {
    }

    public void setRequestMilliseconds(long requestMilliseconds) {
        this.requestMilliseconds = requestMilliseconds;
    }
}