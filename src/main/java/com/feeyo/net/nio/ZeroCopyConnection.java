package com.feeyo.net.nio;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.util.JavaUtils;

/**
 *	ZeroCopy
 *
 *	diskutil list 
 *
 * @author zhuam
 */
public class ZeroCopyConnection extends ClosableConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ZeroCopyConnection.class );
	
	//
	private static final int MAPPED_SIZE = 1024 * 1024 * 2; 
	private static final String MAPPED_SUFFIX = ".ftmpfs";
	
	//
	private static  String MAPPED_PATH = null;
	static {
		// 支持 OPS 指定 tmpfs or ramdisk path
		MAPPED_PATH = System.getProperty("feeyo.zerocopy.path");
		if ( MAPPED_PATH == null ) {
			if ( JavaUtils.isLinux() )  {
				MAPPED_PATH = "/dev/shm"; 	// 在Linux中，用 tmpfs
			} else {
				String path = new File(".").getAbsolutePath();
				MAPPED_PATH =  path.substring(0, path.length() - 1);
			}
		}
		
		// ensure
		ensureDirOK( MAPPED_PATH );
	}
	
	protected AtomicBoolean rwLock = new AtomicBoolean(false); 

    // 映射的文件
	private String fileName;
    private File file;
	private RandomAccessFile randomAccessFile;
	protected FileChannel fileChannel;
	
	// 映射的内存对象
	private MappedByteBuffer mappedByteBuffer;

	//
	public ZeroCopyConnection(SocketChannel channel) {

		super(channel);

		try {
			//
			this.fileName = MAPPED_PATH +  id + MAPPED_SUFFIX;
			this.file = new File( this.fileName );
			
			// mmap
			this.randomAccessFile = new RandomAccessFile(file, "rw");
			this.randomAccessFile.setLength(MAPPED_SIZE);
			this.randomAccessFile.seek(0);

			this.fileChannel = randomAccessFile.getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_SIZE);

		} catch (IOException e) {
			LOGGER.error("create mapped err:", e);
		}
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void register(Selector selector) throws IOException {
		try {	
			processKey = socketChannel.register(selector, SelectionKey.OP_READ, this);
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("register:" + this);
			}
			
			// 已连接、默认不需要认证
	        this.setState( Connection.STATE_CONNECTED );  
			
	        NetSystem.getInstance().addConnection(this);
			if ( this.handler != null )
				this.handler.onConnected( this );
			
		} finally {
			if ( isClosed() ) {
				clearSelectionKey();
			}
		}
	}

	// 异步读取,该方法在 reactor 中被调用
	//
	@SuppressWarnings("unchecked")
	@Override
	public void asynRead() throws IOException {
		
		if (isClosed.get()) {
			return;
		}
		
		//
		if ( !rwLock.compareAndSet(false, true) ) {
			return;
		}
				
		//
		lastReadTime = TimeUtil.currentTimeMillis();
		
		try {
			
			// 循环处理字节信息
			for(;;) {
				
				int position = mappedByteBuffer.position();
				int count    = MAPPED_SIZE - position;
				int length = (int) fileChannel.transferFrom(socketChannel, position, count);
				mappedByteBuffer.position( position + length );
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( length == 0 && count > 0 ){
					length = socketChannel.read(mappedByteBuffer);
				}
				
				if ( length > 0 ) {
					
					//
					netInBytes += length;
					

					// 流量控制
					//
					if ( flowGuard( length ) ) {
						return;
					}
					
					//
					byte[] data = new byte[ length ];
					
					//
					mappedByteBuffer.flip();
					mappedByteBuffer.get(data, 0, length);
					
					// 负责解析报文并处理
					handler.handleReadEvent(this, data);
					
					break;
					
				} else if ( length == 0 ) {
					
					LOGGER.warn("sockect read abnormal, tranfered={}", length);
					
					if (!this.socketChannel.isOpen()) {
						this.close("socket closed");
						return;
					}
					
					// not enough space
					this.mappedByteBuffer.clear();
					
				} else {
					this.close("stream closed");
					return;
				}
			}
			
		} finally {
			rwLock.set(false);	
		}
		
	}

	@Override
	public void write(byte[] buf) {
		
		write( ByteBuffer.wrap(buf) );
	}

	@Override
	public void write(ByteBuffer buf) {
		
		try {
			
			// 
			for (;;) {
				if ( !rwLock.compareAndSet(false, true) ) {
					break;
				}
			}
			
			//
			buf.flip();
			
			int bufSize = buf.limit();
			if ( bufSize <= MAPPED_SIZE ) {
				
				mappedByteBuffer.clear();
				
				int position = 0;
				int count = fileChannel.write(buf, position);
				if ( buf.hasRemaining() ) {
					throw new IOException("can't write whole buffer ,writed " + count + " remains " + buf.remaining());
				}
				int tranfered = write0(position, count);
				
				// 
				netOutBytes += tranfered;
				lastWriteTime = TimeUtil.currentTimeMillis();
				
				// recycle
				NetSystem.getInstance().getBufferPool().recycle(buf);
				
			} else {
				
				// 
				int cnt = ( bufSize / MAPPED_SIZE ) + ( bufSize % MAPPED_SIZE > 0 ? 1 : 0);
				int postion = 0;
				for (int i = 1; i <= cnt; i++) {
					
					int limit = MAPPED_SIZE * i;
					if ( limit > bufSize ) {
						limit = bufSize;
					}
					
					buf.position( postion ); 
					buf.limit( limit ); 
					ByteBuffer tmpBuf = buf.slice();
					
					mappedByteBuffer.clear();
					
					int count = fileChannel.write(tmpBuf, 0);
					if ( tmpBuf.hasRemaining() ) {
						throw new IOException("can't write whole buffer ,writed " + count + " remains " + tmpBuf.remaining());
					}
					
					int tranfered = write0(0, count);
					postion += tranfered;
					
					// 
					netOutBytes += tranfered;
					lastWriteTime = TimeUtil.currentTimeMillis();
				}
				
				// recycle
				NetSystem.getInstance().getBufferPool().recycle(buf);
			}
	
			
		} catch (IOException e) {
			LOGGER.error("write err:", e);
			this.close("write err:" + e);
			
		} finally {
			rwLock.set(false);
		}
	}


	
	private int write0(int position, int count) throws IOException {

		// 往 socketChannel 写入数据
		int tranfered = (int) fileChannel.transferTo(position, count, socketChannel);
		
		boolean noMoreData = tranfered == count;
		if (noMoreData) {
		    if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
		        disableWrite();
		    }

		} else {
		    if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
		        enableWrite(false);
		    }
		}
		
		return tranfered;
	}

	@Override
	public void doNextWriteCheck() {
		// ignore
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
	
	private void closeSocket() {
		if ( socketChannel != null ) {		
			
			if (socketChannel instanceof SocketChannel) {
				Socket socket = ((SocketChannel) socketChannel).socket();
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
				socketChannel.close();
			} catch (Throwable e) {
			}			
			boolean closed = isSocketClosed && (!socketChannel.isOpen());
			if (!closed) {
				LOGGER.warn("close socket of connnection failed " + this);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void close(String reason) {
		//
		if ( !isClosed.get() ) {
			
			closeSocket();
			isClosed.set(true);
		
			//
			this.cleanup();		
			
			//
			NetSystem.getInstance().removeConnection(this);
			if ( this.handler != null )
				this.handler.onClosed(this, reason);
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("close connection, reason:" + reason + " ," + this.toString());
			}
			
			this.attachement = null; //help GC
			
		} else {
		    this.cleanup();
		}
	}
	
	private void cleanup() {
		try {
			unmap(mappedByteBuffer);			
			randomAccessFile.close();
			fileChannel.close();	
			
			if ( file != null ){
				boolean result = this.file.delete();
				LOGGER.info("delete file, name={}, result={}" , this.fileName , result );
			}
			
		} catch (IOException e) {				
			LOGGER.error(" cleanup err: fileName=" + fileName, e);			
		} 	
	}
	
	// unmap
	private void unmap(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction<MappedByteBuffer>() {
			@SuppressWarnings("restriction")
			public MappedByteBuffer run() {
				try {
					Method cleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
					cleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) cleanerMethod.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					LOGGER.error("cannot clean Buffer", e);
				}
				return null;
			}
		});
	}
	
	private static void ensureDirOK(String dirName) {
		
		if ( dirName != null ) {
			
			File f = new File(dirName);
			if (!f.exists()) {
				boolean result = f.mkdirs();
				LOGGER.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
				
			} else {
			
				// delete old mapped
				String[] mfList = f.list( new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.endsWith( MAPPED_SUFFIX );
					}
				});
				
				if (mfList != null &&  mfList.length > 0 ) {
					for(String mf: mfList) {
						File mff = new File( mf );
						if ( mff != null && mff.exists() ) {
							//
							mff.delete();
							//
							LOGGER.info("delete mapped file, fileName={}", mff.getName());
						}
					}
				}
			}
		}
	}
	
	@Override
	public String toString() {
		
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Conn [" );
		sbuffer.append("reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append(":").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", startup=").append( startupTime );
		sbuffer.append(", lastRT=").append( lastReadTime );
		sbuffer.append(", lastWT=").append( lastWriteTime );
		sbuffer.append(", attempts=").append( writeAttempts );	
		sbuffer.append(", isClosed=").append( isClosed );
		sbuffer.append("]");
		return  sbuffer.toString();
	}


}