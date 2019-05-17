package com.feeyo.kafka.net.backend.callback;

import com.feeyo.kafka.codec.ResponseHeader;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.front.RedisFrontConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Responses
 * 
 * Response => CorrelationId ResponseMessage
 *		CorrelationId => int32
 *		ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
 * 
 * 
 * @author yangtao
 *
 */
public abstract class KafkaCmdCallback extends AbstractBackendCallback {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaCmdCallback.class );
	
	protected static final int PRODUCE_RESPONSE_SIZE = 2;
	protected static final int CONSUMER_RESPONSE_SIZE = 3;
	protected static final int OFFSET_RESPONSE_SIZE = 2;

	protected static final byte ASTERISK = '*';
	protected static final byte DOLLAR = '$';
	protected static final byte[] CRLF = "\r\n".getBytes();		
	protected static final byte[] OK =   "+OK\r\n".getBytes();
	protected static final byte[] NULL =   "$-1\r\n".getBytes();
	
	private static int HEAD_LENGTH = 4;
	
	// TODO: 此处待优化，使用 CompositeByteArray
	private byte[] tmpRespBytes;
	
	private void append(byte[] buf) {
		if (tmpRespBytes == null) {
			tmpRespBytes = buf;
		} else {
			byte[] newBytes = new byte[this.tmpRespBytes.length + buf.length];
			System.arraycopy(tmpRespBytes, 0, newBytes, 0, tmpRespBytes.length);
			System.arraycopy(buf, 0, newBytes, tmpRespBytes.length, buf.length);
			this.tmpRespBytes = newBytes;
			newBytes = null;
			buf = null;
		}
	}
	
	// 检查有没有断包
	private boolean isCompletePkg() {
		//
		int len = this.tmpRespBytes.length;
		if (len < HEAD_LENGTH) {
			return false;
		}
		
		int v0 = (this.tmpRespBytes[0] & 0xff) << 24;
		int v1 = (this.tmpRespBytes[1] & 0xff) << 16;  
		int v2 = (this.tmpRespBytes[2] & 0xff) << 8;  
	    int v3 = (this.tmpRespBytes[3] & 0xff); 
	    
	    if (v0 + v1 + v2 + v3 > len - HEAD_LENGTH) {
	    	return false;
	    }
		return true;
	}
	
	@Override
	public void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException {
		
		// 防止断包
		this.append(byteBuff);
		
		if ( !this.isCompletePkg() ) {
			return;
		}
		
		//
		ByteBuffer responseBuf = NetSystem.getInstance().getBufferPool().allocate( this.tmpRespBytes.length );
		try {
			
			// 去除头部的长度
			responseBuf.put(this.tmpRespBytes, HEAD_LENGTH, this.tmpRespBytes.length - HEAD_LENGTH);
			responseBuf.flip();
			
			int responseSize = this.tmpRespBytes.length;
			this.tmpRespBytes = null;
			
			// parse header
			ResponseHeader.parse( responseBuf );
			
			// parse body
			parseResponseBody(conn, responseBuf);
			
			// release
			RedisFrontConnection frontCon = getFrontCon( conn );
			if (frontCon != null) {
				frontCon.releaseLock();
				
				String password = frontCon.getPassword();
				String cmd = frontCon.getSession().getRequestCmd();
				String key = frontCon.getSession().getRequestKey();
				int requestSize = frontCon.getSession().getRequestSize();
				long requestTimeMills = frontCon.getSession().getRequestTimeMills();			
				long responseTimeMills = TimeUtil.currentTimeMillis();
				
				int procTimeMills =  (int)(responseTimeMills - requestTimeMills);
				int backendWaitTimeMills = (int)(conn.getLastReadTime() - conn.getLastWriteTime());

				// 数据收集
				StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills, backendWaitTimeMills, false, false,false);
			}
			
			// 后端链接释放
			conn.release();	
			
		} catch (Exception e) {
		    //TODO  统计和 异常抛出
			LOGGER.error("", e);
		} finally {
			NetSystem.getInstance().getBufferPool().recycle(responseBuf);
		}
		
	}

	// parse body
	public abstract void parseResponseBody(BackendConnection conn, ByteBuffer byteBuff);

}