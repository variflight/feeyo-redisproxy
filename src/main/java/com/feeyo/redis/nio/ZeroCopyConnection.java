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
import java.util.concurrent.ConcurrentLinkedQueue;
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
public class ZeroCopyConnection extends ClosableConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ZeroCopyConnection.class );
	
	private static final boolean IS_LINUX = System.getProperty("os.name").toUpperCase().startsWith("LINUX");
	
	//
	private static final int BUF_SIZE =  1024 ; // 1024 * 1024 * 2;  
	

	protected ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
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
			if ( IS_LINUX ) {
				this.fileName = "/dev/shm/" + id + ".mapped";		// 在Linux中，用 tmpfs
			} else {
				this.fileName =  id + ".mapped";
			} 
			
			this.file = new File( this.fileName );
			
			// ensure
			ensureDirOK(this.file.getParent());
			
			// mmap
			this.randomAccessFile = new RandomAccessFile(file, "rw");
			this.randomAccessFile.setLength(BUF_SIZE);
			this.randomAccessFile.seek(0);

			this.fileChannel = randomAccessFile.getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUF_SIZE);

		} catch (IOException e) {
			LOGGER.error("create mapped err:", e);
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
				int count    = BUF_SIZE - position;
				int tranfered = (int) fileChannel.transferFrom(socketChannel, position, count);
				mappedByteBuffer.position( position + tranfered );
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( tranfered == 0 && count > 0 ){
					tranfered = socketChannel.read(mappedByteBuffer);
				}
				
				if ( tranfered > 0 ) {
					
					//
					byte[] data = new byte[ tranfered ];
					
					mappedByteBuffer.flip();
					mappedByteBuffer.get(data, 0, tranfered);
					
					System.out.println( "asynRead, tranfered="+ tranfered + ",  " + new String(data)  );
					
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
		
		writeQueue.offer(buf);
		
		write();
	}

	private void write() {
		
		try {
			
			while (true) {
				if ( !rwLock.compareAndSet(false, true) ) {
					break;
				}
			}
			
			ByteBuffer buf;
			
			while ((buf = writeQueue.poll()) != null) {
				mappedByteBuffer.clear();
				
				int position = 0;
				int count = fileChannel.write(buf, position);
				if ( buf.hasRemaining() ) {
					throw new IOException("can't write whole buffer ,writed " + count + " remains " + buf.remaining());
				}
				
				write0(position, count);
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			rwLock.set(false);
			if (!writeQueue.isEmpty()) {
				write();
			}
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
	
	@Override
	protected void cleanup() {
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
	
	private void ensureDirOK(final String dirName) {
		if (dirName != null) {
			File f = new File(dirName);
			if (!f.exists()) {
				boolean result = f.mkdirs();
				LOGGER.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
			}
		}
	}

}