package com.feeyo.kafka.net.backend.broker.running;

/**
 * 主发生切换
 */
public interface ServerRunningListener {

    /**
     * 启动时回调做点事情
     */
    public void processStart();

    /**
     * 关闭时回调做点事情
     */
    public void processStop();

    /**
     * 触发现在轮到自己做为active，需要载入上一个active的上下文数据
     */
    public void processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    public void processActiveExit();

}
