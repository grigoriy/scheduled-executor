package com.grigoriyalexeev.pixonic.schexecutor.impl;

import org.joda.time.DateTime;
import org.junit.*;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ScheduledExecutorImplTest {
    @Test
    public void execute_singleTaskInFuture_executesInTime() throws Exception {
        ScheduledExecutorImpl executor = new ScheduledExecutorImpl();
        Callable<DateTime> callable = new Callable<DateTime>() {
            public DateTime call() throws Exception {
                return DateTime.now();
            }
        };
        final int delaySeconds = 10;
        DateTime expectedExecutionTime = DateTime.now().plusSeconds(delaySeconds);

        Future<DateTime> actualExecutionTimeFuture = executor.execute(callable, expectedExecutionTime);
        DateTime actualExecutionTime = actualExecutionTimeFuture.get(delaySeconds * 2, TimeUnit.SECONDS);

        assertEquals((float) expectedExecutionTime.getMillis(), (float) actualExecutionTime.getMillis(), 500);
    }
}