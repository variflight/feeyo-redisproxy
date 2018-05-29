package com.feeyo.kafka.net.backend.broker.zk.running;

/**
 * 主发生切换
 */
public interface ServerRunningListener {

    /**
     * 触发现在轮到自己做为active，需要载入上一个active的上下文数据
     */
    public void processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    public void processActiveExit();

}
