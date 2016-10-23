package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FairScheduledThreadPoolExecutor extends ThreadPoolExecutor implements ScheduledExecutor {
    private final AtomicLong sequence = new AtomicLong();

    public FairScheduledThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize, Integer.MAX_VALUE, 0, NANOSECONDS, new FairTimedBlockingQueue());
        this.prestartAllCoreThreads();
    }

    @Override
    public <T> Future<T> schedule(Callable<T> task, DateTime dateTime) {
        if (task == null || dateTime == null)
            throw new NullPointerException();
        FairScheduledFutureTask<T> t = new FairScheduledFutureTask<>(task, dateTime.getMillis(), sequence.incrementAndGet());
        getQueue().add(t);
        return t;
    }

}
