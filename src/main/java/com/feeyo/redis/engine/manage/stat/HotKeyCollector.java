package com.feeyo.redis.engine.manage.stat;


import com.feeyo.util.topn.Counter;

import java.util.ArrayList;
import java.util.List;

public class HotKeyCollector implements StatCollector {

    private static final LFUCache hotKey = new LFUCache(1000);

    @Override
    public void onCollect(String host, String password, String cmd, String key, int requestSize, int responseSize,
                          int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass) {
        hotKey.set(key,0);
    }


	@Override
	public void onScheduleToZore() {
        hotKey.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}

	public List<Counter<String>> getHotKey(){
        // get 100
        List<Counter<String>> topk = hotKey.getTop();
        if (topk == null) {
            return new ArrayList<>();
        }

        return topk.size() > 100 ? topk.subList(0, 100) : topk;
    }

}
