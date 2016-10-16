package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorImpl implements ScheduledExecutor {
    public Future execute(Callable task, DateTime executionTime) {
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        return scheduledThreadPoolExecutor.schedule(task, executionTime.minus(System.currentTimeMillis()).getMillis(), TimeUnit.MILLISECONDS);
    }
}
