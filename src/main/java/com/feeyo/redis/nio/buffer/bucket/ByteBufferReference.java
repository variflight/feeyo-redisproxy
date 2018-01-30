package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;


/**
 *  用于处理DBB 循环、多重引用 问题
 *
 */
public class ByteBufferReference {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferReference.class );
	
	public final static int _IDLE = 0;
	public final static int _BORROW = 1;
	
	private long address;
	private ByteBuffer buffer;
	
	private AtomicInteger status;
	
	private volatile boolean isMultiReferenced;	// 是否存在多重引用
	private volatile long createTime;
	private volatile long lastTime;
	
	public ByteBufferReference(long address, ByteBuffer bb) {
		this.address = address;
		this.buffer = bb;
		
		this.status = new AtomicInteger(_IDLE);
		this.isMultiReferenced = false;
		this.createTime = TimeUtil.currentTimeMillis();
		this.lastTime = TimeUtil.currentTimeMillis();
	}

	public long getAddress() {
		return address;
	}
	
	public ByteBuffer getByteBuffer() {
		return buffer;
	}

	public void reset() {
		this.isMultiReferenced = false;
		this.status.set( _BORROW );
	}
	
	public boolean isTimeout() {
		return isMultiReferenced && TimeUtil.currentTimeMillis() - lastTime > 30 * 60 * 1000;
	}
	
	public boolean isIdle() {
		if (!isMultiReferenced) {
			if (status.getAndIncrement() != _IDLE) {
				this.isMultiReferenced = true;
				LOGGER.warn("##DBB reference err: {}, address: {}", this, address);
			} else {
				this.lastTime =  TimeUtil.currentTimeMillis();
				return true;
			}
		}
		return false;
	}
	
	public boolean isReferenceOk() {
		if ( !isMultiReferenced ) {
			if (status.getAndDecrement() != _BORROW) {
				this.isMultiReferenced = true;
				LOGGER.warn("##DBB reference err: {}, address: {}", this, address);
			} else {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("buffer:").append(buffer.toString()).append(". createTime:").append(createTime).append(". lastTime:")
				.append(lastTime).append(". status:").append(status.get()).append(". isMultiReferenced:").append(isMultiReferenced)
				.append(".");
		return sb.toString();
	}
}
