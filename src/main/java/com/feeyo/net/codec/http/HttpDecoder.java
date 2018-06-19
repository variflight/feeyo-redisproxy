package com.feeyo.net.codec.http;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import com.feeyo.net.codec.Decoder;
import com.google.common.primitives.Bytes;

/**
 *
 * 解码通过Http post方式的请求内容
 *
 */
public class HttpDecoder implements Decoder<List<ProtobufRequest>> {
	
	private static final Charset charset = Charset.forName("utf-8"); 
	private static final byte[] boundaryPart = "====boundary-part====".getBytes(charset);
	private byte[] _buffer = null;
	private ProtobufRequestDecoder decoder = null;

	public List<ProtobufRequest> decode(byte[] data) {
		
		
		if(data == null)
			return null;
		
		if(_buffer == null || Bytes.indexOf(_buffer, boundaryPart) == -1) {
			append(data);
			int pos = Bytes.indexOf(_buffer, boundaryPart);
			if( pos == -1) 
				return null;
			
			data = Arrays.copyOfRange(_buffer, pos + boundaryPart.length, _buffer.length);
		}
		
		if(decoder == null) {
			decoder = new ProtobufRequestDecoder();
		}
		
		List<ProtobufRequest> reqList = decoder.decode(data);
		if(reqList != null) {
			_buffer = null;
		}
		
		return reqList;
		
	}
	
	private void append(byte[] newBuffer) {
		
		if (newBuffer == null) {
			return;
		}
		
	    if (_buffer == null) {
	      _buffer = newBuffer;
	      return;
	    }
	    
	    // large packet	    	
    	byte[] largeBuffer = new byte[ _buffer.length + newBuffer.length ];
    	System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
    	System.arraycopy(newBuffer, 0, largeBuffer, _buffer.length, newBuffer.length);
    	
    	_buffer = largeBuffer;
	}	

}
