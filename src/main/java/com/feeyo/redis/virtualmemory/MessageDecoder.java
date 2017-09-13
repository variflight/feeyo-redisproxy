package com.feeyo.redis.virtualmemory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class MessageDecoder {
	
    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;
    public final static int COMPRESSED_FLAG = 0x1;
    
    public final static int MESSAGE_MAGIC_CODE_POSTION = 4;
    public final static int MESSAGE_PHYSIC_OFFSET_POSTION = 20;
    public final static int MESSAGE_STORE_TIMESTAMP_POSTION = 40;
    
    public static final int BODY_SIZE_POSITION = 4 // 1 TOTALSIZE
        + 4 // 2 MAGICCODE
        + 4 // 3 BODYCRC
        + 8 // 4 QUEUEID
        + 8 // 5 PHYSICALOFFSET
        + 4 // 6 SYSFLAG
        + 8 // 7 BORNTIMESTAMP
        + 8; // 8 STORETIMESTAMP

    
	public static int calMsgLength(int bodyLength) {
        final int msgLen = 4 										// 1 TOTALSIZE							消息总长度
            + 4 													// 2 MAGICCODE							MESSAGE_MAGIC_CODE
            + 4 													// 3 BODYCRC							消息内容CRC
            + 8 													// 4 QUEUEID							消息队列编号
            + 8 													// 5 PHYSICALOFFSET						物理位置，在 CommitLog 的顺序存储位置
            + 4 													// 6 SYSFLAG
            + 8 													// 7 BORNTIMESTAMP						生成消息时间戳
            + 8 													// 8 STORETIMESTAMP						存储消息时间戳	
            + 4 + (bodyLength > 0 ? bodyLength : 0) 				// 9 BODY								内容长度 + 内容
            + 0;
        return msgLen;
	}


    public static Message decode(java.nio.ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }

    public static Message decode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, false);
    }
    
    public static Message decode(java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody) {
            try {

            	Message msgExt  = new Message();

                // 1 TOTALSIZE
                int storeSize = byteBuffer.getInt();
                msgExt.setStoreSize(storeSize);

                // 2 MAGICCODE
                byteBuffer.getInt();

                // 3 BODYCRC
                int bodyCRC = byteBuffer.getInt();
                msgExt.setBodyCRC(bodyCRC);

                // 4 QUEUEID
                long queueId = byteBuffer.getLong();
                msgExt.setQueueId(queueId);

                // 5 PHYSICALOFFSET
                long physicOffset = byteBuffer.getLong();
                msgExt.setCommitLogOffset(physicOffset);

                // 6 SYSFLAG
                int sysFlag = byteBuffer.getInt();
                msgExt.setSysFlag(sysFlag);

                // 7 BORNTIMESTAMP
                long bornTimeStamp = byteBuffer.getLong();
                msgExt.setBornTimestamp(bornTimeStamp);

                // 8 STORETIMESTAMP
                long storeTimestamp = byteBuffer.getLong();
                msgExt.setStoreTimestamp(storeTimestamp);

                // 9 BODY
                int bodyLen = byteBuffer.getInt();
                if (bodyLen > 0) {
                    if (readBody) {
                        byte[] body = new byte[bodyLen];
                        byteBuffer.get(body);

                        // uncompress body
                        if (deCompressBody && (sysFlag & COMPRESSED_FLAG) == COMPRESSED_FLAG) {
                            body = Util.uncompress(body);
                        }

                        msgExt.setBody(body);
                    } else {
                        byteBuffer.position(byteBuffer.position() + bodyLen);
                    }
                }

                return msgExt;
            } catch (Exception e) {
                byteBuffer.position(byteBuffer.limit());
            }

            return null;
        }


    public static byte[] encode(Message messageExt, boolean needCompress) {
       
    	int sysFlag = messageExt.getSysFlag();
        byte[] body = messageExt.getBody();
        if (needCompress && (sysFlag & COMPRESSED_FLAG) == COMPRESSED_FLAG) {
        	body = Util.compress(body, 5);
        }
        int bodyLength = body.length;
        int storeSize = messageExt.getStoreSize();
        
        ByteBuffer byteBuffer;
        if (storeSize > 0) {
            byteBuffer = ByteBuffer.allocate(storeSize);
        } else {
            storeSize = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 8 // 4 QUEUEID
                + 8 // 5 PHYSICALOFFSET
                + 4 // 6 SYSFLAG
                + 8 // 7 BORNTIMESTAMP
                + 8 // 8 STORETIMESTAMP
                + 4 + bodyLength // 9 BODY
                + 0;
            byteBuffer = ByteBuffer.allocate(storeSize);
        }
        
        byteBuffer.putInt(storeSize);					   		    // 1 TOTALSIZE
        byteBuffer.putInt(MESSAGE_MAGIC_CODE);		        		// 2 MAGICCODE
        byteBuffer.putInt( messageExt.getBodyCRC() );				// 3 BODYCRC
        byteBuffer.putLong( messageExt.getQueueId() );	 			// 4 QUEUEID
        byteBuffer.putLong( messageExt.getCommitLogOffset() );		// 5 PHYSICALOFFSET
        byteBuffer.putInt(sysFlag);									// 6 SYSFLAG
        byteBuffer.putLong( messageExt.getBornTimestamp() );		// 7 BORNTIMESTAMP
        byteBuffer.putLong( messageExt.getStoreTimestamp() );	    // 8 STORETIMESTAMP
        byteBuffer.putInt(bodyLength);								// 9 BODY
        byteBuffer.put( body );
        return byteBuffer.array();
    }

}
