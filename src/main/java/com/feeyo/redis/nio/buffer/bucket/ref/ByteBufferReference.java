package com.feeyo.redis.nio.buffer.bucket.ref;

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
	
	private long address;
	private ByteBuffer buffer;
	
	private AtomicInteger status;				// 0 recycle 、 1 allocate
	
	private volatile boolean isMultiReferenced;	// 是否存在多重引用
	private volatile long createTime;
	private volatile long lastTime;
	
	private static final int TIMEOUT = 30 * 60 * 1000;
	
	public ByteBufferReference(long address, ByteBuffer bb) {
		this.address = address;
		this.buffer = bb;
		
		this.status = new AtomicInteger(0);
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
	
	public boolean isTimeout() {
		return isMultiReferenced && ((TimeUtil.currentTimeMillis() - lastTime) > TIMEOUT);
	}
	
	// 是否可分配
	public boolean isItAllocatable() {
		
		// 是否存在多重引用
		if ( isMultiReferenced ) {
			return false;
		}
		
		// 
		if ( !status.compareAndSet(0, 1) ) {
			this.isMultiReferenced = true;
			LOGGER.warn("##DBB allocate ckh, reference err: {}", this);
			
			return false;
		} else {
			this.lastTime =  TimeUtil.currentTimeMillis();
			return true;
		}
	}
	
	// 是否可以回收的
	public boolean isItRecyclable() {
		
		// 是否存在多重引用
		if ( isMultiReferenced ) {
			return false;
		}
		
		if ( !status.compareAndSet(1, 0) ) { 
			this.isMultiReferenced = true;
			LOGGER.warn("##DBB recycle chk, reference err: {}", this);
			
			return false;
		} else {
			return true;
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("buffer:").append(buffer.toString());
		sb.append(" ,address:").append( address );
		sb.append(" ,createTime:").append(createTime);
		sb.append(" ,lastTime:").append(lastTime);
		sb.append(" ,status:").append(status.get());
		sb.append(" ,isMultiReferenced:").append(isMultiReferenced);
		return sb.toString();
	}
}
