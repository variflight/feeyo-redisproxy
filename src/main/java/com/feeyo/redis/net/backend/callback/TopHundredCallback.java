package com.feeyo.redis.net.backend.callback;

import java.io.IOException;

import com.feeyo.redis.engine.manage.stat.KeyUnit;
import com.feeyo.redis.engine.manage.stat.KeyUnit.KeyType;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.engine.manage.stat.TopHundredProcessUtil;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

public class TopHundredCallback extends AbstractBackendCallback{
	@Override
	public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {
			RedisFrontConnection frontCon = getFrontCon( backendCon );
			String cmd = frontCon.getSession().getRequestCmd();
			byte[] key = frontCon.getSession().getRequestKey();
			String keyStr = new String(key);
			KeyType type = TopHundredProcessUtil.transformCommandToType(cmd);
			String respString = new String(byteBuff);
			KeyUnit keyUnit = TopHundredProcessUtil.handleReceivedString(keyStr, type, respString);
			StatUtil.getTopHundredSet().add(keyUnit);
	}
	
}
