package com.feeyo.redis.nio;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;

/**
 * @author wuzh
 * @author zhuam
 */
public abstract class Connection implements ClosableConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( "Connection" );

	public static final int STATE_CONNECTING = 0;
	public static final int STATE_CONNECTED = 1;
	public static final int STATE_CLOSING = -1;
	public static final int STATE_CLOSED = -2;

	protected String host;
	protected int port;
	protected int localPort;
	protected long id;
	protected String reactor;
	private Object attachement;
	protected int state = STATE_CONNECTING;

	// 连接的方向，in表示是客户端连接过来的，out表示自己作为客户端去连接对端Sever
	public enum Direction {
		in, out
	}

	protected Direction direction = Direction.out;

	protected final SocketChannel channel;

	private SelectionKey processKey;
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	
	
	protected volatile ByteBuffer readBuffer;  //读缓冲区
	protected volatile ByteBuffer writeBuffer; //写缓冲区 及 queue
	protected ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	
	protected AtomicBoolean reading = new AtomicBoolean(false);
	protected AtomicBoolean writing = new AtomicBoolean(false);

	protected long lastLargeMessageTime;
	
	protected static final int maxCapacity = 1024 * 1024 * 16;			// 最大 16 兆
	
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long todoWriteTime;
	
	protected int netInBytes;
	protected int netOutBytes;
	protected int writeAttempts;
	
	protected int pkgTotalSize;
	protected int pkgTotalCount;
	private long idleTimeout;
	
	@SuppressWarnings("rawtypes")
	protected NIOHandler handler;

	public Connection(SocketChannel channel) {
		this.channel = channel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
		this.todoWriteTime = startupTime;
		this.id = ConnectIdGenerator.getINSTNCE().getId();
	}

	public void resetPerfCollectTime() {
		netInBytes = 0;
		netOutBytes = 0;
		pkgTotalCount = 0;
		pkgTotalSize = 0;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}
	

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getId() {
		return id;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
 		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
	}

	public SocketChannel getChannel() {
		return channel;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}
	
	public long getTodoWriteTime() {
		return todoWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public void setHandler(NIOHandler<? extends Connection> handler) {
		this.handler = handler;
	}

	@SuppressWarnings("rawtypes")
	public NIOHandler getHandler() {
		return this.handler;
	}

	public boolean isConnected() {
		boolean isConnected = (this.state != STATE_CONNECTING && state != STATE_CLOSING && state != STATE_CLOSED);
		return isConnected;
	}

	@SuppressWarnings("unchecked")
	public void close(String reason) {
		//
		if ( !isClosed.get() ) {
			
			closeSocket();
			isClosed.set(true);
			
//			this.attachement = null; //help GC
			this.cleanup();		
			NetSystem.getInstance().removeConnection(this);
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("close connection, reason:" + reason + " ," + this.toString());
			}
			
			if (handler != null) {
				handler.onClosed(this, reason);
			}
			
		} else {
		    this.cleanup();
		}
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public void idleCheck() {		
		if ( isIdleTimeout() ) {			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug(toString() + " idle timeout");
			}
			close("idle timeout ");
		} else {
			todoWriteCheck();
		}
	}
	
	/**
	 * 回写检测，如果太慢，此处kill
	 */
	private void todoWriteCheck() {	
		if ( (todoWriteTime - lastWriteTime) > 20000 ) {
			LOGGER.warn(toString() + " warning, too slow.");
			close("warning, too slow.");
		}
	}

	/**
	 * 清理资源
	 */
	protected void cleanup() {
		
		if (readBuffer != null) {
			readBuffer = null;
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
	
	private void clearSelectionKey() {
		try {
			SelectionKey key = this.processKey;
			if (key != null && key.isValid()) {
				key.attach(null);
				key.cancel();
			}
		} catch (Exception e) {
			LOGGER.warn("clear selector keys err:" + e);
		}
	}


	@SuppressWarnings("unchecked")
	public void register(Selector selector) throws IOException {
		try {	
			processKey = channel.register(selector, SelectionKey.OP_READ, this);
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("register:" + this);
			}
			
			// 已连接、默认不需要认证
	        this.setState( Connection.STATE_CONNECTED );  
			NetSystem.getInstance().addConnection(this);

			// 往后通知
			this.handler.onConnected(this);
			
		} finally {
			if ( isClosed() ) {
				clearSelectionKey();
			}
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
	
	
	public void doNextWriteCheck() {
		
		//检查是否正在写,看CAS更新writing值是否成功
		if ( !writing.compareAndSet(false, true) ) {
			return;
		}
		
		try {	
			
			if ( processKey == null ) {
				
				LOGGER.warn( " processKey is null , Thread:" + Thread.currentThread().getName() 
							+ ", \r\n backend: " + this.toSampleString()  + ",  " + System.nanoTime()
							+ ", \r\n front:" +  (this.attachement != null ? ((com.feeyo.redis.net.front.RedisFrontConnection) this.attachement).toSampleString() : null)
							+ "  \r\n");
				return;
			}
			
			//if ( processKey == null ) {
			//	return;
			//}
			
			//利用缓存队列和写缓冲记录保证写的可靠性，返回true则为全部写入成功
			boolean noMoreData = write0();	
				
			lastWriteTime = TimeUtil.currentTimeMillis();
			
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
	
	
	public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;			 // 原始数据长度
		int remaining = buffer.remaining();  // buffer 可写长度
		while (length > 0) {
			if (remaining >= length) {
				buffer.put(src, offset, length);
				break;
			} else {
				buffer.put(src, offset, remaining);				
				writeNotSend(buffer);	
				
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
	
	private final void writeNotSend(ByteBuffer buffer) {
		 writeQueue.offer(buffer);
	}

	public void write(byte[] data) throws IOException {
		int size = data.length;
		if ( size >= NetSystem.getInstance().getBufferPool().getMaxChunkSize() ) {
			size = NetSystem.getInstance().getBufferPool().getMinChunkSize();
		}
		
		ByteBuffer buffer = allocate( size );
		buffer = writeToBuffer(data, buffer);
		write( buffer );
	}

	public static String getStackTrace(Throwable t) {
	    StringWriter sw = new StringWriter();
	    t.printStackTrace(new PrintWriter(sw));
	    return sw.toString();
	}

	
	public void write(ByteBuffer buffer) throws IOException {
		
		/**
		 *  TODO: 防护，避免前端SMEMBERS key，接收延迟严重，造成writeQueue 过大，引起OOM，  
		 *  当发生这种问题后，交由idle 检测服务 kill 前端连接， 起到回收 buffer 的作用
		 */
		this.todoWriteTime = TimeUtil.currentTimeMillis();
		this.writeQueue.offer(buffer);
		
		try {
			this.doNextWriteCheck();
			
		} catch (Exception e) {
			//LOGGER.warn("write err:", e);
			//this.close("write err:" + e);
			throw new IOException( e );
		}
	}
	
	private boolean write0() throws IOException {
		int written = 0;
		ByteBuffer buffer = writeBuffer;
		if (buffer != null) {	
			
			 //只要写缓冲记录中还有数据就不停写入，但如果写入字节为0，证明网络繁忙，则退出
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
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
					written = channel.write(buffer);   // java.io.IOException:
													   // Connection reset by peer
					if (written > 0) {
						lastWriteTime = TimeUtil.currentTimeMillis();
						netOutBytes += written;
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

	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			LOGGER.warn("can't disable write " + this, e);
		}
	}

	private void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("can't enable write: ", e);
		}
		
		if (needWakeup && wakeup) {
			processKey.selector().wakeup();
		}
	}
	
	public void setReactor(String reactorName) {
		this.reactor = reactorName;
	}

	public String getReactor() {
		return this.reactor;
	}

	public boolean belongsReactor(String reacotr) {
		return reactor.equals(reacotr);
	}

	public Object getAttachement() {
		return attachement;
	}

	public void setAttachement(Object attachement) {
		this.attachement = attachement;
	}

	public void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	public void enableRead() {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("enable read fail ", e);
		}
		
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}

	public void setState(int newState) {
		
		this.state = newState;
	}

	/**
	 * 异步读取数据,only nio thread call
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	protected void asynRead() throws IOException {
		
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
				readBuffer = ByteBuffer.allocate( 1024 * 16 );
			}
			
			lastReadTime = TimeUtil.currentTimeMillis();
			
			// 循环处理字节信息
			int offset = 0;
			for (;;) {
				
				if( isClosed() ) {
					return ;
				}
				
				 //从channel中读取数据，并且保存到对应Connection的readBuffer中，readBuffer处于write mode，返回读取了多少字节
				int length = channel.read( readBuffer );
				if ( length == -1 ) {
					this.close("stream closed");
		            return;
				} else if (length == 0 && !this.channel.isOpen()  ) {
					this.close("socket closed");
					return;
				}
				netInBytes += length;
				
				// 空间不足
				if ( !readBuffer.hasRemaining() ) {
					
					if (readBuffer.capacity() >= maxCapacity) {
						LOGGER.warn("con:{},  packet size over the limit.", this);
						throw new IllegalArgumentException( "packet size over the limit.");
					}
					
					// 每次2倍扩充，至 maxCapacity 上限，抛出异常
					int newCapacity = readBuffer.capacity() << 1;
					newCapacity = (newCapacity > maxCapacity) ? maxCapacity : newCapacity;			
					
					ByteBuffer newBuffer = ByteBuffer.allocate( newCapacity );
					
					readBuffer.position( offset );
					newBuffer.put( readBuffer );
					
					readBuffer = newBuffer;
					lastLargeMessageTime = TimeUtil.currentTimeMillis();
					
					// 拿完整包
					continue;		
					//break;		 	
				} 
				
				// 负责解析报文并处理
				int dataLength = readBuffer.position();
				readBuffer.position( offset );
				byte[] data = new byte[ dataLength ];
				readBuffer.get(data, 0, dataLength);

				handler.handleReadEvent(this, data);
				
				
				// 如果当前缓冲区不是 direct byte buffer 
				// 并且最近30秒 没有接收到大的消息 
				// 然后改为直接缓冲 direct byte buffer 提高性能
				if (readBuffer != null && !readBuffer.isDirect() && lastLargeMessageTime != 0
						&& lastLargeMessageTime < (lastReadTime - 30 * 1000L) ) {  // used temp heap
					
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("change to direct con read buffer, cur temp buf size :" + readBuffer.capacity());
					}
					
					ByteBuffer newBuffer = ByteBuffer.allocate( 1024 * 16 );
					readBuffer = newBuffer;
					
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

	private void closeSocket() {
		if ( channel != null ) {		
			
			if (channel instanceof SocketChannel) {
				Socket socket = ((SocketChannel) channel).socket();
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
						LOGGER.error("closeChannelError", e);
					}
				}
			}			
			
			boolean isSocketClosed = true;
			try {
				processKey.cancel();
				channel.close();
			} catch (Throwable e) {
			}			
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (!closed) {
				LOGGER.warn("close socket of connnection failed " + this);
			}
		}
	}

	public int getState() {
		return state;
	}

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Connection.Direction in) {
		this.direction = in;
	}

	public int getPkgTotalSize() {
		return pkgTotalSize;
	}

	public int getPkgTotalCount() {
		return pkgTotalCount;
	}
	
	public String toSampleString() {
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Connection [ " );
		sbuffer.append(" reactor=").append( reactor );
		sbuffer.append(", host=").append( host );
		sbuffer.append(", port=").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", isClosed=").append( isClosed );
		sbuffer.append(", isConnected=").append( isConnected() );
		sbuffer.append(", state=").append( state );
		sbuffer.append("]");
		return  sbuffer.toString();
	}

	@Override
	public String toString() {
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Connection [ " );
		sbuffer.append(", reactor=").append( reactor );
		sbuffer.append(", host=").append( host );
		sbuffer.append(", port=").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", startupTime=").append( startupTime );
		sbuffer.append(", lastReadTime=").append( lastReadTime );
		sbuffer.append(", lastWriteTime=").append( lastWriteTime );
		sbuffer.append(", isClosed=").append( isClosed );
		sbuffer.append("]");
		return  sbuffer.toString();
	}

}