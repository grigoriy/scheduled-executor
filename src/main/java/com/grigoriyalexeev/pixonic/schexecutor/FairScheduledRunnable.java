package com.grigoriyalexeev.pixonic.schexecutor;

import java.util.concurrent.TimeUnit;

public interface FairScheduledRunnable extends Comparable<FairScheduledRunnable>, Runnable {
    long getTime(TimeUnit timeUnit);
}
