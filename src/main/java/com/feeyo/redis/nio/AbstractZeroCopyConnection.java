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
 * @see http://osxdaily.com/2007/03/23/create-a-ram-disk-in-mac-os-x/
 * @see https://www.ibm.com/developerworks/cn/java/j-zerocopy/
 * 
 * @author zhuam
 */
public abstract class AbstractZeroCopyConnection extends AbstractConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractZeroCopyConnection.class );
	
	private static final boolean IS_LINUX = System.getProperty("os.name").toUpperCase().startsWith("LINUX");
	
	//
	private static final int TOTAL_SIZE =  1024 * 1024 * 2;  
	private static final int MARKED = Math.round( TOTAL_SIZE * 0.6F );
	
	//
	private String fileName;
	private RandomAccessFile randomAccessFile;
	protected FileChannel fileChannel;
	private MappedByteBuffer mappedByteBuffer;

	
	// r/w lock
	protected AtomicBoolean LOCK = new AtomicBoolean( false );
	
	
	public AbstractZeroCopyConnection(SocketChannel channel) {

		super(channel);

		try {
			if ( IS_LINUX ) {
				fileName = "/dev/shm/" + id + ".mapped";		// 在Linux中，用 tmpfs
			} else {
				fileName =  id + ".mapped";
			} 
			
			// mmap
			this.randomAccessFile = new RandomAccessFile(fileName, "rw");
			this.randomAccessFile.setLength(TOTAL_SIZE);
			this.randomAccessFile.seek(0);

			this.fileChannel = randomAccessFile.getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_SIZE);

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
		
		//
		if ( !LOCK.compareAndSet(false, true) ) {
			return;
		}
				
		//
		lastReadTime = TimeUtil.currentTimeMillis();
		
		try {
			
			// 循环处理字节信息
			for(;;) {
				
				int oldPos = mappedByteBuffer.position();
				int count    = TOTAL_SIZE - oldPos;
				int tranfered = (int) fileChannel.transferFrom(socketChannel, oldPos, count);
				
				mappedByteBuffer.position( oldPos + tranfered );
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( tranfered == 0 && count > 0 ){
					tranfered = socketChannel.read(mappedByteBuffer);
				}
				
				if ( tranfered > 0 ) {
					
					//
					byte[] data = new byte[ tranfered ];
					
					//
					int newPos = mappedByteBuffer.position();
					mappedByteBuffer.position( oldPos );
					mappedByteBuffer.get(data);
					mappedByteBuffer.position( newPos );
					
//					System.out.println( "tranfered="+ tranfered + ",  " + new String(data)  );
					
					// 负责解析报文并处理
					handler.handleReadEvent(this, data);
					break;
					
				} else if ( tranfered == 0 ) {
					
					LOGGER.warn("sockect read abnormal, tranfered={}", tranfered);
					
					if (!this.socketChannel.isOpen()) {
						this.close("socket closed");
						return;
					}
					
					// not enough space
					rewind();
					
				} else {
					this.close("stream closed");
					return;
				}
			}
			
		} finally {
			LOCK.compareAndSet(true, false);
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
	}

	@Override
	public void doNextWriteCheck() {
		// ignore
	}

	// 
	private void rewind() {
		int pos = this.mappedByteBuffer.position();
		if ( pos > MARKED ) {
			
			LOGGER.info("mapped bytebuffer rewind, pos={}, marked={}", pos, MARKED);
			
			this.mappedByteBuffer.compact(); // 压缩,舍弃position之前的内容
			this.mappedByteBuffer.position(0);
			this.mappedByteBuffer.limit( pos );
		}
	}
	
	@Override
	protected void cleanup() {
		try {
			mappedByteBuffer.rewind();
			
			unmap(mappedByteBuffer);			
			randomAccessFile.close();
			fileChannel.close();	
			
			// 删除文件
			File file = new File( fileName );
			if ( file.exists() )
				file.delete();		
			
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

}