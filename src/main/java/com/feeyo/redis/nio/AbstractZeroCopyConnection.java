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
	
	/*
	  mappedByteBuffer 目前没有rewind， read & write 操作都始终在前移为止，没有回转，
	  最好的做法是发现已经使用了1/2的容量（或2/3）后，以及不够写数据的情况下，rewind回去
	  
	  redis client ------ ( r/w buffer ) ------>  front connection
	  back connection  ------ ( r/w buffer )  ------> redis server
	  
	  1、read socket data, adjust writePos
	  2、write byte buffer, adjust writePos 
	  3、write data to socket, adjust readPos
	 */
	private int totalSize =  1024 * 1024 * 1;  
	private int readPos;
	private int writePos;
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
		if ( isOutOfBounds() )
			rewind();
		
		try {
			
			// 循环处理字节信息
			for(;;) {
				
				final int position = mappedByteBuffer.position();
				final int count    = totalSize - position;
				int tranfered = (int) fileChannel.transferFrom(channel, position, count);
				
				// 写入位置
				writePos = position + tranfered;
				mappedByteBuffer.position( writePos );
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( tranfered == 0 && count > 0 ){
					tranfered = channel.read(mappedByteBuffer);
				}
				
				if ( tranfered > 0 ) {
					
					// 负责解析报文并处理
					byte[] data = new byte[ tranfered ];
					
					//
					int oldPos = mappedByteBuffer.position();
					mappedByteBuffer.position( readPos );
					
					ByteBuffer copyBuf = mappedByteBuffer.slice();
					copyBuf.limit( tranfered );
					
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
			
			if ( isOutOfBounds() )
				rewind();
			
			int position = mappedByteBuffer.position();
			int writed = fileChannel.write(buf, position);
			if ( buf.hasRemaining() ) {
				throw new IOException("can't write whole buffer ,writed " + writed + " remains " + buf.remaining());
			}
			
			// 写入位置
			writePos = position + writed;
			mappedByteBuffer.position( writePos );
			
			write0();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 往 socketChannel 写入数据
	private void write0() throws IOException {
		
		int writeEnd = mappedByteBuffer.position();
		int writeCount = writeEnd - readPos;
		int writed = (int) fileChannel.transferTo(readPos, writeCount, channel);
		this.readPos += writed;		
		
		LOGGER.info("#{} write data to socket, writed = {}, readPos = {}, writePos = {}",
				new Object[]{ fileName, writed, readPos, writePos });
		
		boolean noMoreData = writed == writeCount;
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
	private boolean isOutOfBounds() {
		if (readPos > marked || writePos > marked) {
			LOGGER.info("#{} buffer is out of bounds, readPos = {}, writePos = {}, totalSize = {}, marked = {}  ",
					new Object[] { fileName, readPos, writePos, totalSize, marked });
			return true;
		}
		return false;
	}

	//
	private void rewind() {

		int length = writePos - readPos;

		this.mappedByteBuffer.position(readPos);
		this.mappedByteBuffer.limit(writePos);
		this.mappedByteBuffer.compact(); // 压缩,舍弃position之前的内容

		this.readPos = 0;
		this.writePos = length;
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