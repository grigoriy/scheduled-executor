package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class FairScheduledFixedThreadPoolExecutor implements ScheduledExecutor {
    private final FairTimedBlockingQueue queue;
    private final ScheduledFixedThreadPoolExecutor executor;
    private final AtomicLong idSequence;

    public FairScheduledFixedThreadPoolExecutor(int numThreads) {
        queue = new FairTimedBlockingQueue();
        executor = new ScheduledFixedThreadPoolExecutor(numThreads, queue);
        idSequence = new AtomicLong();
    }

    @Override
    public <T> Future<T> schedule(Callable<T> task, DateTime dateTime) {
        if (task == null || dateTime == null)
            throw new NullPointerException();
        FairScheduledFutureTask<T> future = new FairScheduledFutureTask<>(task, dateTime.getMillis(), idSequence.incrementAndGet());
        executor.execute(future);
        return future;
    }
}
