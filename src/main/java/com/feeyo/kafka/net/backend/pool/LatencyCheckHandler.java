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

        // kafka heartbeat
        if (byteBuff.length >= 4 && isOk(byteBuff)) {
            long cost = System.currentTimeMillis() - requestMilliseconds;
            LatencyCollector.add(nodeId, epoch, cost);
            conn.release();

        } else {
            conn.close("heartbeat err");
        }
    }

    private boolean isOk(byte[] buffer) {
        int len = buffer.length;
        if (len < 4) {
            return false;
        }
        int v0 = (buffer[0] & 0xff) << 24;
        int v1 = (buffer[1] & 0xff) << 16;
        int v2 = (buffer[2] & 0xff) << 8;
        int v3 = (buffer[3] & 0xff);

        if (v0 + v1 + v2 + v3 > len - 4) {
            return false;
        }

        return true;
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