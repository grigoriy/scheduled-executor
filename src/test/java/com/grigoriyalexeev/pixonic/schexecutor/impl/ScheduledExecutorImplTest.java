package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScheduledExecutorImplTest {
    @Test
    public void execute_singleTaskInFuture_executesInTime() throws Exception {
        ScheduledExecutor executor = getExecutor(5);
        Callable<Long> callable = createMillisCallable();
        final int delaySeconds = 1;
        DateTime expectedExecutionTime = DateTime.now().plusSeconds(delaySeconds);

        Future<Long> actualExecutionTimeFuture = executor.schedule(callable, expectedExecutionTime);
        Long actualExecutionTime = actualExecutionTimeFuture.get(delaySeconds, TimeUnit.SECONDS);

        assertEquals((float) expectedExecutionTime.getMillis(), (float) actualExecutionTime, 0);
    }

    @Test
    public void execute_singleTaskInPast_executesImmediately() throws Exception {
        ScheduledExecutor executor = getExecutor(5);
        Callable<Long> callable = createNanosCallable();
        final int delaySeconds = 1;
        DateTime expectedExecutionTime = DateTime.now().minusSeconds(delaySeconds);

        Future<Long> actualExecutionTimeFuture = executor.schedule(callable, expectedExecutionTime);
        Long actualExecutionTime = actualExecutionTimeFuture.get(delaySeconds, TimeUnit.SECONDS);

        assertEquals((float) System.nanoTime(), (float) actualExecutionTime, 0);
    }

    @Test
    public void execute_multiplePastTasksEqualTime_executeImmediatelyInFifoOrder() throws Exception {
        final int numTasks = 10;
        final int delayMillis = 10;
        final int numTrials = 100;
        int wrongTimingsCounter = 0;

        for (int j = 0; j < numTrials; j++) {
            ScheduledExecutor executor = getExecutor(5);
            DateTime expectedExecutionTime = DateTime.now().minusSeconds(delayMillis);
            List<Future<Long>> actualExecutionTimeFutures = new ArrayList<Future<Long>>();

            for (int i = 0; i < numTasks; i++) {
                actualExecutionTimeFutures.add(executor.schedule(createNanosCallable(), expectedExecutionTime));
            }

            for (int i = 1; i < actualExecutionTimeFutures.size(); i++) {
                Long earlierTask = actualExecutionTimeFutures.get(i - 1).get();
                Long laterTask = actualExecutionTimeFutures.get(i).get();
                if (earlierTask > laterTask) {
                    wrongTimingsCounter++;
                }
            }
        }

        assertEquals(0, wrongTimingsCounter);
    }

    @Test
    public void execute_twoTasksLaterThanEarlier_executeInTimeOrder() throws Exception {
        final int delayMillisSleepTask = -10;
        final int delayMillisLater = 20;
        final int delayMillisEarlier = 0;
        final int numTrials = 100;

        for (int j = 0; j < numTrials; j++) {
            ScheduledExecutor executor = getExecutor(5);
            DateTime expectedExecutionTimeSleep = DateTime.now().plusMillis(delayMillisSleepTask);
            DateTime expectedExecutionTimeLater = DateTime.now().plusMillis(delayMillisLater);
            DateTime expectedExecutionTimeEarlier = DateTime.now().plusMillis(delayMillisEarlier);
            Callable<Void> sleepTask = () -> {
                Thread.sleep(10);
                return null;
            };
            executor.schedule(sleepTask, expectedExecutionTimeSleep);
            Future<Long> laterTaskActualExecutionTimeFuture = executor.schedule(createNanosCallable(), expectedExecutionTimeLater);
            Future<Long> earlierTaskActualExecutionTimeFuture = executor.schedule(createNanosCallable(), expectedExecutionTimeEarlier);

            assertTrue("Task with earlier planned execution time must be executed earlier.",
                    earlierTaskActualExecutionTimeFuture.get() < laterTaskActualExecutionTimeFuture.get());
        }
    }

    private static Callable<Long> createMillisCallable() {
        return System::currentTimeMillis;
    }

    private static Callable<Long> createNanosCallable() {
        return System::nanoTime;
    }

    private static ScheduledExecutor getExecutor(int numThreads) {
//        return new ScheduledExecutorImpl(numThreads);
        return new FairScheduledThreadPoolExecutor(1);
    }
}