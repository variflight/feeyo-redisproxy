package com.feeyo.redis.nio;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;

/**
 * ZeroCopy
 * 
 * @author zhuam
 */
public abstract class AbstractZeroCopyConnection extends AbstractConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractZeroCopyConnection.class );
	
	private int totalSize =  1024 * 1024 * 2;  
	private int marked = Math.round( totalSize * 0.6F );
	
	//
	private String fileName;
	private RandomAccessFile randomAccessFile;
	protected FileChannel fileChannel;
	private MappedByteBuffer mappedByteBuffer;

	
	// r/w lock
	protected AtomicBoolean rwLock = new AtomicBoolean( false );
	
	// OS
	private static boolean IS_LINUX = System.getProperty("os.name").toUpperCase().startsWith("LINUX");
	
	
	public AbstractZeroCopyConnection(SocketChannel channel) {

		super(channel);

		try {
			
			if ( IS_LINUX ) {
				fileName = "/dev/shm/" + id + ".mapped";
			} else {
				fileName =  id + ".mapped";
			} 
			
			this.randomAccessFile = new RandomAccessFile(fileName, "rw");
			this.randomAccessFile.setLength(totalSize);
			this.randomAccessFile.seek(0);

			this.fileChannel = randomAccessFile.getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);

		} catch (IOException e) {
			LOGGER.error("create mapped err:", e);
		}
		
	}

	/**
	 * 异步读取,该方法在 reactor 中被调用
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void asynRead() throws IOException {
		
		if (isClosed.get()) {
			return;
		}
		
		// rw 进行中
		if ( !rwLock.compareAndSet(false, true) ) {
			return;
		}
				
		//
		lastReadTime = TimeUtil.currentTimeMillis();
		
		//
		rewind();
		
		try {
			
			// 循环处理字节信息
			for(;;) {
				
				final int position = mappedByteBuffer.position();
				final int count    = totalSize - position;
				int tranfered = (int) fileChannel.transferFrom(channel, position, count);
				
				mappedByteBuffer.position( position + tranfered );
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( tranfered == 0 && count > 0 ){
					tranfered = channel.read(mappedByteBuffer);
				}
				
				if ( tranfered > 0 ) {
					
					// 负责解析报文并处理
					
					
					//
					int oldPos = mappedByteBuffer.position();
					mappedByteBuffer.position( 0 );
					
					// 
					ByteBuffer copyBuf = mappedByteBuffer.slice();
					copyBuf.limit( tranfered );
					
					byte[] data = new byte[ tranfered ];
					copyBuf.get(data);
					
					mappedByteBuffer.position(oldPos);
					
					System.out.println( "tranfered="+ tranfered + ",  " + new String(data)  );
					
					//
					handler.handleReadEvent(this, data);
					break;
					
				} else if ( tranfered == 0 ) {
					
					if (!this.channel.isOpen()) {
						this.close("socket closed");
						return;
					}
					
					// not enough space
					//
					
				} else {
					this.close("stream closed");
					return;
				}
				
			}
			
		} finally {
			rwLock.set( false );
		}
		
	}

	@Override
	public void write(byte[] buf) {
		write( ByteBuffer.wrap(buf) );
	}

	@Override
	public void write(ByteBuffer buf) {
		
		// TODO 
		// 1、考虑 rwLock 自旋
		// 2、考虑 buf size 大于 mappedByteBuffer 的情况，分块
		// 3、 write0 确保写OK
		
		try {
			
			rewind();
			
			int position = mappedByteBuffer.position();
			int count = fileChannel.write(buf, position);
			if ( buf.hasRemaining() ) {
				throw new IOException("can't write whole buffer ,writed " + count + " remains " + buf.remaining());
			}
			
			//
			mappedByteBuffer.position( position + count );
			
			write0( position, count );
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 往 socketChannel 写入数据
	private void write0(int position, int count) throws IOException {
		
		int writed = (int) fileChannel.transferTo(position, count, channel);
		
		boolean noMoreData = writed == count;
		if (noMoreData) {
		    if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
		        disableWrite();
		    }

		} else {
		    if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
		        enableWrite(false);
		    }
		}
	}

	@Override
	public void doNextWriteCheck() {
		// ignore
	}

	//
	private void rewind() {
		int pos = this.mappedByteBuffer.position();
		if ( pos > marked ) {
			
			this.mappedByteBuffer.position(0);
			this.mappedByteBuffer.limit( pos );
			this.mappedByteBuffer.compact(); // 压缩,舍弃position之前的内容
		}
	}
	
	@Override
	protected void cleanup() {
		try {
			cleanMapped(mappedByteBuffer);			
			randomAccessFile.close();
			channel.close();	
			
			// 删除文件
			File file = new File( fileName );
			file.delete();		
			
		} catch (IOException e) {				
			LOGGER.error(" cleanup err: fileName=" + fileName, e);			
		} 	
	}
	
	
	// clean mapped
	private void cleanMapped(final Object buffer) {
		AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@SuppressWarnings("restriction")
			public Object run() {
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

}