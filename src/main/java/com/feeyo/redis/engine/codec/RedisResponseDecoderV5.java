package com.feeyo.redis.engine.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 返回  type+data+\r\n 字节流， 避免 encode   
 * 
 * @see https://redis.io/topics/protocol
 *
 */
public class RedisResponseDecoderV5 extends AbstractDecoder {
	
	
	private List<RedisResponse> responses = null;
	
	public List<RedisResponse> decode(ByteBuffer buffer) {

		append(buffer);

		try {
			if ( responses != null ) {
				for (RedisResponse rr : responses) {
					rr.cleanup();
				}
				responses.clear();
			} else {
				responses = new ArrayList<RedisResponse>(2);
			}
			
			for(;;) {
				
				// 至少4字节  :1\r\n
		        if (bytesRemaining() < 4) {
		          return null;
		        }
				
		        byte type = _buffer.get(_offset++);
		        switch (type) {
		          case '*':					// 数组(Array), 以 "*" 开头,表示消息体总共有多少行（不包括当前行）, "*" 是具体行数
		          case '+':					// 正确, 表示正确的状态信息, "+" 后就是具体信息		
		          case '-':					// 错误, 表示错误的状态信息, "-" 后就是具体信息
		          case ':':					// 整数, 以 ":" 开头, 返回
		          case '$':					// 批量字符串, 以 "$" 开头,表示下一行的字符串长度,具体字符串在下一行中,字符串最大能达到512MB  
		            RedisResponse resp = parseResponse(type);
		            if ( resp != null ) {
		            	responses.add( resp );
		            }
		        }
		        
		        if (_buffer.position() < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");
					
				} else if (_buffer.position() == _offset) {
					cleanup();
	            		return responses;
				}
			}
			
		} catch (IndexOutOfBoundsException e1) {
			// 捕获这个错误（没有足够的数据），等待下一个数据包 
	        _offset = 0;
	       
	        if ( responses != null ) {
				for (RedisResponse rr : responses) {
					rr.cleanup();
				}
				responses.clear();
			}
	        return null;
		}		
		
	}
	
	private RedisResponse parseResponse(byte type) {

		int start, end, len, offset;
		
		if (type == '+' || type == '-' || type == ':') {			
			
			offset = _offset;			
			start = offset - 1;
			end = readCRLFOffset();	// 分隔符 \r\n			
			_offset = end; 			// 调整偏移值
			
			if (end > _buffer.position() ) {
				_offset = offset;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}
			

			// 长度
			len = end - start;
			return new RedisResponse(type, getByteBuffer(start, len));
			
		 } else if (type == '$') {
			 
			 offset = _offset;
		      
		      // 大小为 -1的数据包被认为是 NULL
		      int packetSize = readInt();
		      if (packetSize == -1) {
		    	start = offset - 1;
		    	end = _offset;
		    	len = end - start;
		        return new RedisResponse(type, getByteBuffer(start, len));  // 此处不减
		      }

		      end = _offset + packetSize + 2;	// offset + data + \r\n
		      _offset = end;					// 调整偏移值		
		      
		      if (end > _buffer.position()) {
		        _offset = offset - 1;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }
		      
		      start = offset - 1;
		      len = end - start;
		      return new RedisResponse(type, getByteBuffer(start, len));
			 
		 } else if (type == '*') {
			 
			  offset = _offset;
		    
		      // 大小为 -1的数据包被认为是 NULL
			  int packetSize = readInt();
		      if (packetSize == -1) {
		    	start = offset -1;
		    	end = _offset;
		    	len = end - start;
		        return new RedisResponse(type, getByteBuffer(start, len));  // 此处不减
		      }

		      if (packetSize > bytesRemaining()) {
		        _offset = offset - 1;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }

		      // 此处多增加一长度，用于存储 *packetSize\r\n
		      RedisResponse response = new RedisResponse(type, packetSize + 1);
		      start = offset -1;
		      end = _offset;
		      len = end - start;
		      response.set(0, new RedisResponse(type, getByteBuffer(start, len)));

		      byte ntype;
		      RedisResponse res;
		      for (int i = 1; i <= packetSize; i++) {
		        if (_offset + 1 >= _buffer.position() ) {
		        		response.cleanup();
		        		throw new IndexOutOfBoundsException("Wait for more data.");
		        }

		        ntype = _buffer.get(_offset++);
		        try {
		        		res = parseResponse( ntype );
		        		response.set(i, res);
		        } catch (IndexOutOfBoundsException e) {
		        		response.cleanup();
		        		throw e;
		        }
		      }
		      return response;
		 }

		return null;
	}
	
	public static void main(String[] args) {
		byte[] buf = ("*200\r\n" + 
				"$13\r\n" + 
				"8bqmlpagwc938\r\n" + 
				"$827\r\n" + 
				"xvxql7xa7dgo324getgorp8rt5p78uqjihlr6kiygo5bc0849d3cmqvurfq33r4973cj3i7it6z9u5hxdq7vjqr81whkttpas0y31642nt01ddinfwsii3evhqu5nbwmcpgmfkdu3puk6n478wfl8fyllkm2k3bq1hpumomzjxtah9gn6lfbdvw5rzgh5nctyoxk2lyvhlbatdjxe4sxokcz5e0wuoz2jfs7u12w3a6dv40oh0v3a85ktyyoa76nwprcuyaxz7hvm2ek5hnseskexlrotubkprlrrai23dxjbnkdow0l1kizfu21sq0jegi71bdj97oelrzz4kyam3cytqwjnvy5r0bdn3gxkpsflu3v3umm2fp7y8vhplgrc0vtx8nawsxri4zojdnyhbbi55dsq3dodviy8nasw4tuh1hu225uxu8r4c1gcfssq64pwwdmyze8e8z5z7kx96yqq9w4ksuv8kiaovz4lrvcwrqt97t8bxhwjxl5c5nx1m3814l6xsx7z81aarvrv0z83rgnw7u26mv0qy7kxtmswtuddlfl2k26ajptjb5fjnn90r5bf2cwno782h7vsvn6zq5k6ber4al1m5q56vmnzmxpo61pcuec8o2wxseemqcytu00imxpsvwbx0hurx34c4n4ymx8z45glg13b88xon86o8xn439nbqdx0l2lxumkps1yp33couwy917vz6pvdyi7tl65avxu5bul0imtawh0cvy8yhnqq5hd11x1js9yynihc26tq03ulry0ab3njb20ljnf9kvzrxngxyi0n0m2spojeu1yej6\r\n" + 
				"$13").getBytes();
		ByteBuffer bb = ByteBuffer.allocate(buf.length);
		bb.put(buf);
		RedisResponseDecoderV5 v5 = new RedisResponseDecoderV5();
		v5.decode(bb);
		
	}
	
}