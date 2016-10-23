package com.grigoriyalexeev.pixonic.schexecutor.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduledFixedThreadPoolExecutor extends ThreadPoolExecutor {
    public ScheduledFixedThreadPoolExecutor(int numThreads, BlockingQueue<Runnable> queue) {
        super(numThreads, numThreads, 0, TimeUnit.SECONDS, queue);
    }

    @Override
    public void execute(Runnable task) {
        getQueue().add(task);
        prestartCoreThread();
    }
}
