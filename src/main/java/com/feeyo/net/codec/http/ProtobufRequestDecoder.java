package com.feeyo.net.codec.http;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.UnknowProtocolException;

public class ProtobufRequestDecoder implements Decoder<List<ProtobufRequest>> {

	private static Logger LOGGER = LoggerFactory.getLogger(ProtobufRequestDecoder.class);
	private byte[] _buffer;
	private boolean isCustomPkg;
	private int protoType = -1;
	private Decoder<List<ProtobufRequest>> decoder = null;
	

	public List<ProtobufRequest> decode(byte[] data) {
		
		if(data == null)
			return null;
		
		if(protoType == -1) {
			append(data);
			if(_buffer.length < 4 + 1)
				return null;
			protoType = _buffer[3] & 0xFF | (_buffer[2] & 0xFF) << 8
					| (_buffer[1] & 0xFF) << 16 | (_buffer[0] & 0xFF) << 24;
			isCustomPkg = _buffer[4] == 0x00 ? true : false;
			data = Arrays.copyOfRange(_buffer, 5, _buffer.length);
		}

		try {
			switch(protoType) {
			case ProtobufMsgType.Erapb_Message_Type:
				if(decoder == null) {
					decoder = new MessageDecoder(isCustomPkg);
				}
				return decoder.decode(data);
			//TODO expand
			default:
				return null;
			}
		} catch (UnknowProtocolException e) {
			LOGGER.error(e.getMessage());
		}
		return null;
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
