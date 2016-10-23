package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.FairScheduledRunnable;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FairScheduledFutureTask<V> extends FutureTask<V> implements FairScheduledRunnable {
    private final long time;
    private final long sequenceNumber;

    FairScheduledFutureTask(Callable<V> callable, long ns, long sequenceNumber) {
        super(callable);
        this.time = ns;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public long getTime(TimeUnit unit) {
        return unit.convert(time, MILLISECONDS);
    }

    @Override
    public int compareTo(FairScheduledRunnable other) {
        if (other == this) {
            return 0;
        }
        if (other instanceof FairScheduledFutureTask) {
            FairScheduledFutureTask<?> x = (FairScheduledFutureTask<?>)other;
            long diff = time - x.time;
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else if (sequenceNumber < x.sequenceNumber)
                return -1;
            else
                return 1;
        }
        long diff = getTime(MILLISECONDS) - other.getTime(MILLISECONDS);
        return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }
}
