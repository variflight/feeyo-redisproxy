package com.feeyo.redis.net.front.rewrite.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.rewrite.KeyIllegalException;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategy;

/**
 * 变换第一个Key
 * @author zhuam
 *
 */
public class FirstKey extends KeyRewriteStrategy {

	@Override
	public void rewriteKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
        int numArgs = request.getNumArgs();
        if (numArgs < 2) {
            return;
        }
		byte[][] args = request.getArgs();
		
		checkIllegalCharacter(userCfg.getKeyRule(), args[1]);
		//修改前缀
		args[1] = concat(userCfg.getPrefix(), args[1]);

        //添加默认过期时间
        byte[] cmd = args[0];
        if (cmd.length != 3) {
            return;
        }
        //针对Set 指令 string
        if ((cmd[0] == 'S' || cmd[0] == 's') && (cmd[1] == 'E' || cmd[1] == 'e') && (cmd[2] == 'T' || cmd[2] == 't')) {
            byte[] exBytes = "EX".getBytes();
            if (numArgs == 3) {
                byte[][] newArgs = new byte[][]{args[0], args[1], args[2], exBytes, userCfg.getExpireTime()};
                request.setArgs(newArgs);
            } else if (numArgs == 4) {
                byte[][] newArgs = new byte[][]{args[0], args[1], args[2], exBytes, userCfg.getExpireTime(), args[3]};
                request.setArgs(newArgs);
            }
        }

	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() < 2) {
			return null;
		}
		byte[][] args = request.getArgs();
		return args[1];
	}

}
