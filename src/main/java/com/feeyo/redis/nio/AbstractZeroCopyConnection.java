package com.feeyo.redis.nio;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
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

	private static final int TOTAL_SIZE = 1024 * 1024 * 1;  

	//
	private RandomAccessFile randomAccessFile;
	protected FileChannel fileChannel;
	private MappedByteBuffer mappedByteBuffer;

	protected long position;
	protected long count;
	
	// r/w lock
	protected AtomicBoolean rwLock = new AtomicBoolean( false );

	public AbstractZeroCopyConnection(SocketChannel channel) {

		super(channel);

		try {
			this.randomAccessFile = new RandomAccessFile(id + ".mapped", "rw");
			this.randomAccessFile.setLength(TOTAL_SIZE);

			this.fileChannel = randomAccessFile.getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_SIZE);

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 异步读取,该方法在 reactor 中被调用
	 */
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
		
		try {
			
			// 循环处理字节信息
			for(;;) {
				
				final int position = mappedByteBuffer.position();
				final int count    = TOTAL_SIZE - position;
				int tranfered = (int) fileChannel.transferFrom(channel, position, count);
				mappedByteBuffer.position(position + tranfered);
				
				// fixbug: transferFrom() always return 0 when client closed abnormally!
				// --------------------------------------------------------------------
				// So decide whether the connection closed or not by read()! 
				if( tranfered == 0 && count > 0 ){
					tranfered = channel.read(mappedByteBuffer);
				}
				
				switch ( tranfered ) {
				case 0:
					if (!this.channel.isOpen()) {
						this.close("socket closed");
						return;
					}
					
					// not enough space
					// 
					
					break;
				case -1:
					this.close("stream closed");
					break;
				default:
					
					//
					// 负责解析报文并处理
					
					break;
				}
			}
			
		} finally {
			rwLock.set( false );
		}
		
	}

	@Override
	public void write(byte[] buf) {
		
		mappedByteBuffer.put(buf);

		try {
			write0();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(ByteBuffer buf) {
		
		
		try {
			
			int position = mappedByteBuffer.position();
			int writed = fileChannel.write(mappedByteBuffer, position);
			
			if (buf.hasRemaining()) {
				throw new IOException("can't write whole buf ,writed " + writed + " remains " + buf.remaining());
			}
			mappedByteBuffer.position(position + writed);
			
			write0();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void write0() throws IOException {
		
		//
		fileChannel.transferTo(position, count, channel);
		
		boolean noMoreData = false;
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
	
	
	@Override
	protected void cleanup() {
		// clear buffer
		try {
			if ( randomAccessFile != null )
				randomAccessFile.close();
			
			if ( fileChannel != null)
				fileChannel.close();
		} catch (IOException e) {
			// ignore
		}		
	}

}
