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
        final int delaySeconds = 10;
        DateTime expectedExecutionTime = DateTime.now().plusSeconds(delaySeconds);
        System.out.println(expectedExecutionTime);

        Future<Long> actualExecutionTimeFuture = executor.schedule(callable, expectedExecutionTime);
        Long actualExecutionTime = actualExecutionTimeFuture.get(delaySeconds + 1, TimeUnit.SECONDS);
        System.out.println(new DateTime(actualExecutionTime));

        final long difference = Math.abs(expectedExecutionTime.getMillis() - actualExecutionTime);
        final int delta = 1;
        assertTrue(String.format("Difference between expected and actual execution time is [%s] and is more than delta [%s]", difference, delta),
                difference <= delta);
    }

    @Test
    public void execute_singleTaskInPast_executesImmediately() throws Exception {
        ScheduledExecutor executor = getExecutor(5);
        Callable<Long> callable = createMillisCallable();
        final int delaySeconds = 1;
        DateTime scheduledExecutionTime = DateTime.now().minusSeconds(delaySeconds);

        Future<Long> actualExecutionTimeFuture = executor.schedule(callable, scheduledExecutionTime);
        Long expectedExecutionTime = System.currentTimeMillis();
        Long actualExecutionTime = actualExecutionTimeFuture.get(delaySeconds, TimeUnit.SECONDS);

        final long difference = Math.abs(expectedExecutionTime - actualExecutionTime);
        final int delta = 100;
        assertTrue(String.format("Difference between expected and actual execution time is [%s] and is more than delta [%s]", difference, delta),
                difference <= delta);
    }

    @Test
    public void execute_multiplePastTasksEqualTime_executeImmediatelyInFifoOrder() throws Exception {
        final int numTasks = 10;
        final int delayMillis = 10;
        final int numTrials = 1;
        final int numThreads = 5;
        int wrongTimingsCounter = 0;

        for (int j = 0; j < numTrials; j++) {
            DateTime expectedExecutionTime = DateTime.now().minusSeconds(delayMillis);
            List<Future<Long>> actualExecutionTimeFutures = new ArrayList<>();
            ScheduledExecutor executor = getExecutor(numThreads);

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
    public void execute_twoTasksLaterAndEarlier_executeInTimeOrder() throws Exception {
        final int delayMillisLater = 100;
        final int delayMillisEarlier = 0;
        final int numTrials = 100;

        for (int j = 0; j < numTrials; j++) {
            ScheduledExecutor executor = getExecutor(5);
            DateTime expectedExecutionTimeLater = DateTime.now().plusMillis(delayMillisLater);
            DateTime expectedExecutionTimeEarlier = DateTime.now().plusMillis(delayMillisEarlier);
            Future<Long> laterTaskActualExecutionTimeFuture = executor.schedule(createNanosCallable(), expectedExecutionTimeLater);
            Future<Long> earlierTaskActualExecutionTimeFuture = executor.schedule(createNanosCallable(), expectedExecutionTimeEarlier);

            assertTrue("Task with earlier planned execution time must be executed earlier.",
                    earlierTaskActualExecutionTimeFuture.get() <= laterTaskActualExecutionTimeFuture.get());
        }
    }

    private static Callable<Long> createMillisCallable() {
        return () -> {
            long callTime = System.currentTimeMillis();
            spin(5);
            return callTime;
        };
    }

    private static Callable<Long> createNanosCallable() {
        return () -> {
            long callTime = System.nanoTime();
            spin(5);
            return callTime;
        };
    }

    private static ScheduledExecutor getExecutor(int numThreads) {
//        final ScheduledExecutor executor = new ScheduledExecutorImpl(numThreads);
//        final ScheduledExecutor executor = new FairScheduledThreadPoolExecutor(numThreads);
        final ScheduledExecutor executor = new FairScheduledFixedThreadPoolExecutor(numThreads);
        // warm up
        for (int i = 0; i < numThreads; i++) {
            final DateTime scheduledTime = DateTime.now().minusSeconds(10);
            executor.schedule(() -> {
                spin(20);
                return System.nanoTime();
            }, scheduledTime);
        }
        return executor;
    }

    private static void spin(int milliseconds) {
        long sleepTime = milliseconds*1000000L; // convert to nanoseconds
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) < sleepTime) {}
    }
}