package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.nio.util.TimeUtil;

public class ByteBufferState {
	public final static int STATE_IDLE = 0;
	public final static int STATE_BORROW = 1;
	
	private AtomicInteger state;
	private boolean isHealthy;
	private ByteBuffer byteBuffer;
	private long createTime;
	private long lastUseTime;
	
	public ByteBufferState(ByteBuffer bb) {
		this.state = new AtomicInteger(STATE_IDLE);
		this.byteBuffer = bb;
		this.isHealthy = true;
		this.createTime = TimeUtil.currentTimeMillis();
		this.lastUseTime = TimeUtil.currentTimeMillis();
	}

	public AtomicInteger getState() {
		return state;
	}

	public void setState(AtomicInteger state) {
		this.state = state;
	}

	public boolean isHealthy() {
		return isHealthy;
	}

	public void setHealthy(boolean isHealthy) {
		this.isHealthy = isHealthy;
	}

	public ByteBuffer getByteBuffer() {
		return byteBuffer;
	}

	public void setByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public long getLastUseTime() {
		return lastUseTime;
	}

	public void setLastUseTime(long lastUseTime) {
		this.lastUseTime = lastUseTime;
	}
	
	public boolean isUnHealthyTimeOut() {
		return !isHealthy() && TimeUtil.currentTimeMillis() - lastUseTime > 30 * 60 * 1000;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("buffer:").append(byteBuffer.toString()).append(". create:").append(createTime).append(". last use:")
				.append(lastUseTime).append(". use:").append(state.get()).append(". isHealthy:").append(isHealthy)
				.append(".");
		return sb.toString();
	}
}
