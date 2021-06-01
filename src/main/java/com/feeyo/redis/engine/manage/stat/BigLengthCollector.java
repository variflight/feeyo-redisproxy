package com.feeyo.redis.engine.manage.stat;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.PoolType;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BigLengthCollector implements StatCollector {

    private static Logger LOGGER = LoggerFactory.getLogger(BigLengthCollector.class);

    private final static int THRESHOLD = 10000;

    private final static int SIZE_OF_1K = 1024;
    private final static int SIZE_OF_10K = 10 * SIZE_OF_1K;


    // key -> password,cmd，key
    private static ConcurrentHashMap<String, String[]> waitForConfirmKeyMap = new ConcurrentHashMap<String, String[]>();

    // 已确认 big length
    private static ConcurrentHashMap<String, BigLength> bLengthKeyMap = new ConcurrentHashMap<String, BigLength>();

    private static long lastCheckTime = TimeUtil.currentTimeMillis();
  
    
    private static AtomicBoolean isChecking = new AtomicBoolean(false);
    
    //
    private void detection() {

        if (!isChecking.compareAndSet(false, true)) 
            return;
        
        //
        try {

            lastCheckTime = TimeUtil.currentTimeMillis();

            // 按照物理节点分组 然后发送 验证
            Map<PhysicalNode, List<String[]>> nodeToKeysMap = new HashMap<>(14);
            for (String[] item : waitForConfirmKeyMap.values()) {

            	String password = item[0];
                String cmd = item[1];
                String key = item[2];
                //
                UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(password);
                if (userCfg == null) {
                    continue;
                }
                
                //
                AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get(userCfg.getPoolId());
                PhysicalNode physicalNode = (pool.getType() == PoolType.REDIS_CLUSTER ? 
                		pool.getPhysicalNode(cmd, key) : pool.getPhysicalNode());
                if ( physicalNode == null ) {
                    continue;
                }
                
                //
                List<String[]> itemList = nodeToKeysMap.get(physicalNode);
                if (itemList == null) {
                    itemList = new ArrayList<>();
                    nodeToKeysMap.put(physicalNode, itemList);
                }
                itemList.add( new String[]{ cmd, key } );
            }

            // 按主机进行校验
            for (java.util.Map.Entry<PhysicalNode, List<String[]>> entry : nodeToKeysMap.entrySet()) {

                //
                JedisConnection conn = null;
                try {
                    // 前置设置 readonly
                    conn = new JedisConnection(entry.getKey().getHost(), entry.getKey().getPort(), 1000, 1000);
                    
                    //
                    for (String[] item: entry.getValue()) {

                    	String cmd = item[0];
                        String key = item[1];
                        //
                        if (cmd.equals("HMSET") ||
                                cmd.equals("HSET") ||
                                cmd.equals("HSETNX") ||
                                cmd.equals("HINCRBY") ||
                                cmd.equals("HINCRBYFLOAT")) { // hash

                            conn.sendCommand(RedisCommand.HLEN, key);

                        } else if (cmd.equals("LPUSH") ||
                                cmd.equals("LPUSHX") ||
                                cmd.equals("RPUSH") ||
                                cmd.equals("RPUSHX")) { // list

                            conn.sendCommand(RedisCommand.LLEN, key);

                        } else if (cmd.equals("SADD")) { // set

                            conn.sendCommand(RedisCommand.SCARD, key);

                        } else if (cmd.equals("ZADD") ||
                                cmd.equals("ZINCRBY") ||
                                cmd.equals("ZREMRANGEBYLEX")) { // sortedset

                            conn.sendCommand(RedisCommand.ZCARD, key);
                        }

                        // 获取集合长度
                        long length = conn.getIntegerReply();
                        if (length > THRESHOLD) {

                            BigLength bLength = bLengthKeyMap.get(key);
                            if (bLength == null) {
                                bLength = new BigLength();
                                bLength.cmd = cmd;
                                bLength.key = key;
                                String[] value = waitForConfirmKeyMap.get(key);
                                if (value != null) {
                                    bLength.password = value[0];
                                    bLengthKeyMap.put(key, bLength);
                                }
                            }
                            bLength.length.set((int) length);

                        } else {
                            bLengthKeyMap.remove(key);
                        }

                        waitForConfirmKeyMap.remove(key);

                        // ###########################################
                        if (bLengthKeyMap.size() > 100) {
                        	//
                            BigLength min = null;
                            for (BigLength bigLen : bLengthKeyMap.values()) {
                                if (min == null) {
                                    min = bigLen;
                                } else {
                                    if (bigLen.length.get() < min.length.get()) {
                                        min = bigLen;
                                    }
                                }
                            }
                            bLengthKeyMap.remove(min.key);
                        }
                    }
                } catch (Exception e2) {
                    LOGGER.error("big length chk err:", e2);
                } finally {
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
            //
            nodeToKeysMap.clear();
            
        } finally {
            isChecking.set(false);
        }
    }


    public ConcurrentHashMap<String, BigLength> getBigLengthMap() {
        return bLengthKeyMap;
    }


    @Override
    public void onCollect(String host, String password, String cmd, String key, int requestSize, int responseSize,
                          int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass) {

        // 统计集合类型key
        if (cmd.equals("HMSET")    // hash
                || cmd.equals("HSET")
                || cmd.equals("HSETNX")
                || cmd.equals("HINCRBY")
                || cmd.equals("HINCRBYFLOAT")

                || cmd.equals("LPUSH")    // list
                || cmd.equals("LPUSHX")
                || cmd.equals("RPUSH")
                || cmd.equals("RPUSHX")

                || cmd.equals("SADD") // set

                || cmd.equals("ZADD")  // sortedset
                || cmd.equals("ZINCRBY")
                || cmd.equals("ZREMRANGEBYLEX")) {


            BigLength bigLength = bLengthKeyMap.get(key);
            if (bigLength != null) {
                if (requestSize > SIZE_OF_10K) {
                    bigLength.count_10k.incrementAndGet();

                } else if (requestSize > SIZE_OF_1K) {
                    bigLength.count_1k.incrementAndGet();
                }
            }

            //
            if (!waitForConfirmKeyMap.containsKey(key)) {
            	//
                if (waitForConfirmKeyMap.size() < 1000) {
                    waitForConfirmKeyMap.put(key, new String[]{password, cmd, key});
                } else {
                    detection();
                }
            }
        }

    }


    @Override
    public void onScheduleToZore() {
    	//
        ConcurrentHashMap<String, String[]> tmp = new ConcurrentHashMap<String, String[]>();
        for (BigLength bigLength : bLengthKeyMap.values()) {
            tmp.put(bigLength.key, new String[]{bigLength.password, bigLength.cmd, bigLength.key});
        }

        //
        waitForConfirmKeyMap.putAll(tmp);
    }

    @Override
    public void onSchedulePeroid(int peroid) {
        if (TimeUtil.currentTimeMillis() - lastCheckTime >= peroid * 1000 * 10) {
            detection();
        }
    }

    public static class BigLength {
        public String password;
        public String cmd;
        public String key;
        public AtomicInteger length = new AtomicInteger(0);
        public AtomicInteger count_1k = new AtomicInteger(0);
        public AtomicInteger count_10k = new AtomicInteger(0);
    }
}
