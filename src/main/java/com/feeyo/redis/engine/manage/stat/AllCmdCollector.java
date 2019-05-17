package com.feeyo.redis.engine.manage.stat;


import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.net.nio.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AllCmdCollector implements StatCollector {

    private static Logger logger = LoggerFactory.getLogger(AllCmdCollector.class);

    /**
     * 开始时间
     */
    private volatile long startTime=0;

    /**
     * 结束时间
     */
    private volatile long endTime=0;

    private volatile long size = 10000;

    private ConcurrentLinkedQueue<String> linkedQueue=new ConcurrentLinkedQueue();
    private static final String format = "yyyy-MM-dd HH:mm:ss";

    /**
     *  设置统计时间段及每次打印的size
     * @param start
     * @param end
     * @param size
     * @return
     */
    public boolean setStatTime(String start, String end, String size) {
        boolean result = true;
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            this.startTime = dateFormat.parse(start).getTime();
            this.endTime = dateFormat.parse(end).getTime();
            if (size != null && !size.isEmpty()) {
                this.size = Integer.parseInt(size);
            }
        } catch (ParseException e) {
            result = false;
        }
        return result;
    }


   private final String info="p=%s c=%s k=%s r=%s p=%s pT=%s wT=%s";

    @Override
    public void onCollect(String password, String cmd, String key, int requestSize, int responseSize,
                          int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass) {
        long time = TimeUtil.currentTimeMillis();
        if (time < startTime) {
            return;
        }
        if (time < endTime) {
            linkedQueue.add(String.format(info, password, cmd, key, requestSize, responseSize, procTimeMills, waitTimeMills));
            if (linkedQueue.size() > size) {
                logger.info(JsonUtils.marshalToString(linkedQueue));
                linkedQueue.clear();
            }
        } else {
            if (linkedQueue.size() == 0) {
                return;
            }
            logger.info(JsonUtils.marshalToString(linkedQueue));
            linkedQueue.clear();
        }
    }


	@Override
	public void onScheduleToZore() {

	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}


}
