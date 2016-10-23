package com.grigoriyalexeev.pixonic.schexecutor;

import java.util.concurrent.TimeUnit;

public interface FairScheduledRunnable<V> extends Comparable<FairScheduledRunnable>, Runnable {
    long getTime(TimeUnit timeUnit);
}
