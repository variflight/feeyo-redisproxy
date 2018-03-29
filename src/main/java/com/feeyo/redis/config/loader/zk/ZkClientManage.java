package com.feeyo.redis.config.loader.zk;

import com.feeyo.redis.net.codec.RedisRequest;

/**
 * redis proxy's zk command handler
 *
 * <p>supports:<pre>
 *    <strong> zk upload user.xml </strong>
 *    <strong> zk upload server.xml </strong>
 *    <strong> zk upload pool.xml </strong>
 *    <strong> zk activation false </strong>
 *    <strong> zk activation true </strong>
 *
 * @author Tr!bf wangyamin@variflight.com
 *
 */
public class ZkClientManage {
    
    public static byte[] execute(RedisRequest request) {
        
        boolean opState;

        byte[][] args = request.getArgs();
        if (request.getNumArgs() != 3) {
            return "-ERR wrong number of arguments\r\n".getBytes();
        }
        
        if (!ZkClient.INSTANCE().isOnline()) {
            return "-ERR ZkClient is offline\r\n".getBytes();
        }

        String subCmd = new String(args[1]).toUpperCase();

        switch (subCmd) {
            case "UPLOAD":
                String fileName = new String(args[2]);
                opState = ZkClient.INSTANCE().upload2zk(fileName);
                return opState ? "+OK\r\n".getBytes() : "-ERR wrong file name\r\n".getBytes();
            case "ACTIVATION":
                String auto = new String(args[2]).toUpperCase();
                switch (auto) {
                    case "TRUE":
                        opState = ZkClient.INSTANCE().resetAutoActivation(true);
                        break;
                    case "FALSE":
                        opState = ZkClient.INSTANCE().resetAutoActivation(false);
                        break;
                    default :
                        opState = false;
                        break;
                }
                return opState ? "+OK\r\n".getBytes() : "-ERR wrong arguments\r\n".getBytes();
            default:
                return "-ERR zk [upload <config file name like user.xml>] [activation <true|false>]\r\n".getBytes();
        }
    }
}
