package com.feeyo.redis.engine.manage.stat;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HotKeyCollector implements StatCollector {

    private static Logger logger = LoggerFactory.getLogger(HotKeyCollector.class);

    private static final LFUCache<String, Integer> hotKey = new LFUCache<>(1000, 0.75f);


    @Override
    public void onCollect(String host, String password, String cmd, String key, int requestSize, int responseSize,
                          int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass) {
        hotKey.incr(key,1);
    }


	@Override
	public void onScheduleToZore() {
        hotKey.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}

	public  Map<String,Integer> getHotKey(){
        // get 100
        return hotKey.getTop100();
    }

}
