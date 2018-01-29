package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;


/**
 *  direct byte buffer reference check
 *
 */
public class ByteBufferReference {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferReference.class );
	
	public final static int _IDLE = 0;
	public final static int _BORROW = 1;
	
	private long address;
	private ByteBuffer byteBuffer;
	
	private AtomicInteger code;
	
	private volatile boolean isHealthy;
	private volatile long createTime;
	private volatile long lastTime;
	
	public ByteBufferReference(long address, ByteBuffer bb) {
		this.address = address;
		this.byteBuffer = bb;
		
		
		this.code = new AtomicInteger(_IDLE);
		this.isHealthy = true;
		this.createTime = TimeUtil.currentTimeMillis();
		this.lastTime = TimeUtil.currentTimeMillis();
	}
	
	

	public long getAddress() {
		return address;
	}
	
	public ByteBuffer getByteBuffer() {
		return byteBuffer;
	}

	public void reset() {
		this.isHealthy = true;
		this.code.set( _BORROW );
	}
	
	public boolean isHealthy() {
		return isHealthy;
	}


	
	public boolean isTimeout() {
		return !isHealthy() && TimeUtil.currentTimeMillis() - lastTime > 30 * 60 * 1000;
	}
	
	public boolean isBorrow(long address) {
		if (isHealthy) {
			if (code.getAndIncrement() != _IDLE) {
				this.isHealthy = false;
				LOGGER.warn(
						"Direct ByteBuffer allocate warning.... allocate buffer that is been used。ByteBufferState: {}, address: {}",
						this, address);
			} else {
				this.lastTime =  TimeUtil.currentTimeMillis();
				return true;
			}
		}
		return false;
	}
	
	public boolean isIdle(long address) {
		if (isHealthy) {
			if (code.getAndDecrement() != _BORROW) {
				this.isHealthy = false;
				LOGGER.warn("DirectByteBuffer reference err: {},address: {}", this, address);
			} else {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("buffer:").append(byteBuffer.toString()).append(". create:").append(createTime).append(". last use:")
				.append(lastTime).append(". use:").append(code.get()).append(". isHealthy:").append(isHealthy)
				.append(".");
		return sb.toString();
	}
}
