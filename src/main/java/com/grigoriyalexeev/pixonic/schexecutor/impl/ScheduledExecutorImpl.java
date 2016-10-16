package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorImpl implements ScheduledExecutor {
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public ScheduledExecutorImpl(Integer numThreads) {
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(numThreads);
    }

    public <T>Future<T> schedule(Callable<T> task, DateTime executionTime) {
        return scheduledThreadPoolExecutor.schedule(task, executionTime.minus(System.currentTimeMillis()).getMillis(), TimeUnit.MILLISECONDS);
    }
}
