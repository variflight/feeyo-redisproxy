package com.feeyo.net.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;

/**
 * new connection
 * 
 * @author zhuam
 */
public class Connection extends ClosableConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( Connection.class );
	
	protected volatile ByteBuffer readBuffer;  //读缓冲区
	protected volatile ByteBuffer writeBuffer; //写缓冲区 及 queue
	protected ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	
	protected AtomicBoolean reading = new AtomicBoolean(false);
	protected AtomicBoolean writing = new AtomicBoolean(false);

	protected long lastLargeMessageTime;
	protected long largeCounter;
	
	protected static final int maxCapacity = 1024 * 1024 * 16;			// 最大 16 兆
	

	public Connection(SocketChannel socketChannel) {
		super(socketChannel);
	}

	// 最后扩容时间
	public long getLastLargeMessageTime() {
		return lastLargeMessageTime;
	}

	// 扩容的次数
	public long getLargeCounter() {
		return largeCounter;
	}

	// 清理资源
	@Override
	protected void cleanup() {
		
		if (readBuffer != null) {
			recycle(readBuffer);
			this.readBuffer = null;
		}
		
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}
		
		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}
	
	// 内部
	private ByteBuffer allocate(int chunkSize) {
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( chunkSize );
		return buffer;
	}
	
	private final void recycle(ByteBuffer buffer) {
		NetSystem.getInstance().getBufferPool().recycle(buffer);
	}
	
	@Override
	public void doNextWriteCheck() {
		
		
		// you need sure write queue is not empty
		if ( writeQueue.isEmpty() ) {
			return;
		}
		
		//检查是否正在写,看CAS更新writing值是否成功
		if ( !writing.compareAndSet(false, true) ) {
			return;
		}
		
		try {
			//利用缓存队列和写缓冲记录保证写的可靠性，返回true则为全部写入成功
			boolean noMoreData = write0();	
			
		    //如果全部写入成功而且写入队列为空（有可能在写入过程中又有新的Bytebuffer加入到队列），则取消注册写事件
            //否则，继续注册写事件
			if ( noMoreData && writeQueue.isEmpty() ) {
				if ( (processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					disableWrite();
				}
			} else {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					enableWrite(false);
				}
			}
			
		} catch (IOException e) {
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("caught err:", e);
			}
			close("err:" + e);
		} finally {
			//CAS RESET
			writing.set(false);	
		}
	}

	
	private ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;			 // 原始数据长度
		int remaining = buffer.remaining();  // buffer 可写长度
		while (length > 0) {
			if (remaining >= length) {
				buffer.put(src, offset, length);
				break;
			} else {
				buffer.put(src, offset, remaining);				
				writeQueue.offer(buffer); // write not send
				
				int chunkSize = NetSystem.getInstance().getBufferPool().getMinChunkSize();
				buffer = allocate( chunkSize );
				offset += remaining;
				length -= remaining;
				remaining = buffer.remaining();
				continue;
			}
		}
		return buffer;
	}

	// data ->  N 个 minChunk buffer
	@Override
	public void write(byte[] data) {
		if (data == null)
			return;
		
		int size = data.length;
		if ( size >= NetSystem.getInstance().getBufferPool().getDecomposeBufferSize() ) {
			size = NetSystem.getInstance().getBufferPool().getMinChunkSize();
		}
		
		ByteBuffer buffer = allocate( size );
		buffer = writeToBuffer(data, buffer);
		write( buffer );
		data = null;
	}

	@Override
	public void write(ByteBuffer data) {
		
		this.writeQueue.offer( data );
		
		try {
			this.doNextWriteCheck();
			
		} catch (Exception e) {
			LOGGER.error("write err:", e);
			this.close("write err:" + e);
			//throw new IOException( e );
		}
	}
	
	private boolean write0() throws IOException {
		
		int written = 0;
		ByteBuffer buffer = writeBuffer;
		if (buffer != null) {	
			
			 //只要写缓冲记录中还有数据就不停写入，但如果写入字节为0，证明网络繁忙，则退出
			while (buffer.hasRemaining()) {
				written = socketChannel.write(buffer);
				if (written > 0) {
					netOutCounter++;
					netOutBytes += written;
					lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}

			//如果写缓冲中还有数据证明网络繁忙，计数并退出，否则清空缓冲
			if (buffer.hasRemaining()) {
				writeAttempts++;
				return false;
			} else {
				writeBuffer = null;
				recycle( buffer );
			}
		}
		
		//读取缓存队列并写channel
		while ((buffer = writeQueue.poll()) != null) {
			if (buffer.limit() == 0) {
				recycle(buffer);
				close("quit send");
				return true;
			}

			buffer.flip();
			try {
				while (buffer.hasRemaining()) {
					written = socketChannel.write(buffer);   // java.io.IOException:
													   // Connection reset by peer
					if (written > 0) {
						lastWriteTime = TimeUtil.currentTimeMillis();
						netOutCounter++;
						netOutBytes += written;
						lastWriteTime = TimeUtil.currentTimeMillis();
					} else {
						break;
					}
				}
			} catch (IOException e1) {
				recycle(buffer);
				throw e1;
			} 
			
			 //如果写缓冲中还有数据证明网络繁忙，计数，记录下这次未写完的数据到写缓冲记录并退出，否则回收缓冲
			if (buffer.hasRemaining()) {
				writeBuffer = buffer;
				writeAttempts++;
				return false;
			} else {
				recycle(buffer);
			}
		}
		return true;
	}


	/**
	 * 异步读取,该方法在 reactor 中被调用
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void asynRead() throws IOException {
		
		if (isClosed.get()) {
			return;
		}
		
		//检查是否正在写,看CAS更新reading值是否成功
		if ( !reading.compareAndSet(false, true) ) {
			LOGGER.info(" connection reading cas ... ");
			return;
		}
		
		try {
		
			//如果buffer为空，证明被回收或者是第一次读，新分配一个buffer给 Connection作为readBuffer
			if ( readBuffer == null) {
				// readBuffer = ByteBuffer.allocate( 1024 * 16 );
				
				if ( direction == Direction.in )
					readBuffer = allocate( 1024 * 16 );
				else
					readBuffer = allocate( 1024 * 64 );
			}
			
			lastReadTime = TimeUtil.currentTimeMillis();
			
			// 循环处理字节信息
			int offset = 0;
			for (;;) {
				
				if( isClosed() ) {
					return ;
				}
				
				 //从channel中读取数据，并且保存到对应Connection的readBuffer中，readBuffer处于write mode，返回读取了多少字节
				int length = socketChannel.read( readBuffer );
				if ( length == -1 ) {
					this.close("stream closed");
		            return;
				} else if (length == 0 && !this.socketChannel.isOpen()  ) {
					this.close("socket closed");
					return;
				}
				netInBytes += length;
				netInCounter++;
				
				
				// 流量控制
				//
				if ( isNested ) {
					
					if ( parent.getHandler().handleNetFlow(parent, length)  ) {
						parent.flowClean();
						return;
					}	
					
				} else {
					
					//
					if ( handler.handleNetFlow(this, length) ) {
						this.flowClean();
						return;
					}
				}
				
				// 空间不足
				if ( !readBuffer.hasRemaining() ) {
					
					if (readBuffer.capacity() >= maxCapacity) {
						LOGGER.warn("con:{},  packet size over the limit.", this);
						throw new IllegalArgumentException( "packet size over the limit.");
					}
					
					// 每次2倍扩充，至 maxCapacity 上限，抛出异常
					int newCapacity = readBuffer.capacity() << 1;
					newCapacity = (newCapacity > maxCapacity) ? maxCapacity : newCapacity;			
					
					// new buffer
					ByteBuffer newBuffer = allocate( newCapacity );
					readBuffer.position( offset );
					newBuffer.put( readBuffer );
					
					recycle(readBuffer);
					readBuffer = newBuffer;
					lastLargeMessageTime = TimeUtil.currentTimeMillis();
					largeCounter++;
					
					// 拿完整包
					//continue;		
//					break;		// 对 大包不友好 	
				} 
				
				// 负责解析报文并处理
				int dataLength = readBuffer.position();
				readBuffer.position( offset );
				byte[] data = new byte[ dataLength ];
				readBuffer.get(data, 0, dataLength);

				if ( isNested )
					handler.handleReadEvent(parent, data);
				else
					handler.handleReadEvent(this, data);
				
				
				// 存在扩大后的 byte buffer
				// 并且最近30秒 没有接收到大的消息 
				// 然后改为直接缓冲 direct byte buffer 提高性能
				
				// if (readBuffer != null && !readBuffer.isDirect() && lastLargeMessageTime != 0
				//		&& lastLargeMessageTime < (lastReadTime - 30 * 1000L) ) {  
					
				if (readBuffer != null && lastLargeMessageTime != 0 && lastLargeMessageTime < (lastReadTime - 30 * 1000L) ) {  

					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("change to direct con read buffer, cur temp buf size :" + readBuffer.capacity());
					}
					
					ByteBuffer oldBuffer = readBuffer;
					ByteBuffer newBuffer = allocate( 1024 * 16 );  // ByteBuffer.allocate( 1024 * 16 );
					readBuffer = newBuffer;
					
					//
					if ( oldBuffer.isDirect() )
						recycle( oldBuffer );
					
					lastLargeMessageTime = 0;
					
				} else {
					if (readBuffer != null) {
						readBuffer.clear();
					}
				}
				
				// no more data ,break
				break;
			}
			
			
		} finally {
			//CAS RESET
			reading.set(false);	
		}

	}
	

	@Override
	public String toString() {
		
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Conn[" );
		sbuffer.append("reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append(":").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", startup=").append( startupTime );
		sbuffer.append(", lastRT=").append( lastReadTime );
		sbuffer.append(", lastWT=").append( lastWriteTime );
		sbuffer.append(", attempts=").append( writeAttempts );	//
		sbuffer.append(", cc=").append( netInCounter ).append("/").append( netOutCounter );	
		
		if ( isClosed.get() ) {
			sbuffer.append(", isClosed=").append( isClosed );
			sbuffer.append(", closedTime=").append( closeTime );
			sbuffer.append(", closeReason=").append( closeReason );
		}
		
		sbuffer.append("]");
		return  sbuffer.toString();
	}

}