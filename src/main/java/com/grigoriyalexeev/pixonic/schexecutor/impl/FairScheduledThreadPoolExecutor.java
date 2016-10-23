package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.FairScheduledRunnable;
import com.grigoriyalexeev.pixonic.schexecutor.ScheduledExecutor;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
        FairScheduledFutureTask<T> t = new FairScheduledFutureTask<T>(task, dateTime.getMillis(), sequence.incrementAndGet());
        super.getQueue().add(t);
        return t;
    }

    public class FairScheduledFutureTask<V> extends FutureTask<V> implements FairScheduledRunnable<V> {
        private final long sequenceNumber;
        private long time;

        FairScheduledFutureTask(Callable<V> callable, long ns, long sequenceNumber) {
            super(callable);
            this.time = ns;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public long getTime(TimeUnit unit) {
            return unit.convert(time - now(), NANOSECONDS);
        }

        @Override
        public int compareTo(FairScheduledRunnable other) {
            if (other == this) // compare zero if same object
                return 0;
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
            long diff = getTime(NANOSECONDS) - other.getTime(NANOSECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled)
                remove(this);
            return cancelled;
        }
    }

    public static class FairTimedBlockingQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {
        private final transient ReentrantLock lock = new ReentrantLock();
        private final PriorityQueue<FairScheduledRunnable> q = new PriorityQueue<>();
        private Thread leader = null;
        private final Condition available = lock.newCondition();

        @Override
        public boolean add(Runnable e) {
            return offer(e);
        }

        @Override
        public boolean offer(Runnable e) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                q.offer((FairScheduledRunnable) e);
                if (q.peek() == e) {
                    leader = null;
                    available.signal();
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void put(Runnable e) {
            offer(e);
        }

        @Override
        public boolean offer(Runnable e, long timeout, TimeUnit unit) {
            return offer(e);
        }

        @Override
        public FairScheduledRunnable poll() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                FairScheduledRunnable first = q.peek();
                if (first == null || System.nanoTime() < first.getTime(TimeUnit.NANOSECONDS))
                    return null;
                else
                    return q.poll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public FairScheduledRunnable take() throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                for (;;) {
                    FairScheduledRunnable first = q.peek();
                    if (first == null)
                        available.await();
                    else {
                        long time = first.getTime(NANOSECONDS);
                        if (System.nanoTime() - time >= 0)
                            return q.poll();
                        first = null; // don't retain ref while waiting
                        if (leader != null)
                            available.await();
                        else {
                            Thread thisThread = Thread.currentThread();
                            leader = thisThread;
                            try {
                                available.awaitNanos(System.nanoTime() - time);
                            } finally {
                                if (leader == thisThread)
                                    leader = null;
                            }
                        }
                    }
                }
            } finally {
                if (leader == null && q.peek() != null)
                    available.signal();
                lock.unlock();
            }
        }

        @Override
        public FairScheduledRunnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                for (;;) {
                    FairScheduledRunnable first = q.peek();
                    if (first == null) {
                        if (nanos <= 0)
                            return null;
                        else
                            nanos = available.awaitNanos(nanos);
                    } else {
                        long scheduledTime = first.getTime(NANOSECONDS);
                        if (scheduledTime <= System.nanoTime())
                            return q.poll();
                        if (nanos <= 0)
                            return null;
                        first = null; // don't retain ref while waiting
                        if (nanos < (System.nanoTime() - scheduledTime) || leader != null)
                            nanos = available.awaitNanos(nanos);
                        else {
                            Thread thisThread = Thread.currentThread();
                            leader = thisThread;
                            try {
                                long delay = System.nanoTime() - scheduledTime;
                                long timeLeft = available.awaitNanos(delay);
                                nanos -= delay - timeLeft;
                            } finally {
                                if (leader == thisThread)
                                    leader = null;
                            }
                        }
                    }
                }
            } finally {
                if (leader == null && q.peek() != null)
                    available.signal();
                lock.unlock();
            }
        }

        @Override
        public FairScheduledRunnable peek() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return q.peek();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int size() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return q.size();
            } finally {
                lock.unlock();
            }
        }

        private FairScheduledRunnable peekExpired() {
            // assert lock.isHeldByCurrentThread();
            FairScheduledRunnable first = q.peek();
            return (first == null || System.nanoTime() > first.getTime(NANOSECONDS)) ?
                    null : first;
        }

        @Override
        public int drainTo(Collection<? super Runnable> c) {
            if (c == null)
                throw new NullPointerException();
            if (c == this)
                throw new IllegalArgumentException();
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int n = 0;
                for (Runnable e; (e = peekExpired()) != null;) {
                    c.add(e);       // In this order, in case add() throws.
                    q.poll();
                    ++n;
                }
                return n;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int drainTo(Collection<? super Runnable> c, int maxElements) {
            if (c == null)
                throw new NullPointerException();
            if (c == this)
                throw new IllegalArgumentException();
            if (maxElements <= 0)
                return 0;
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int n = 0;
                for (Runnable e; n < maxElements && (e = peekExpired()) != null;) {
                    c.add(e);       // In this order, in case add() throws.
                    q.poll();
                    ++n;
                }
                return n;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void clear() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                q.clear();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int remainingCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public Object[] toArray() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return q.toArray();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public <T> T[] toArray(T[] a) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return q.toArray(a);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean remove(Object o) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return q.remove(o);
            } finally {
                lock.unlock();
            }
        }

        void removeEQ(Object o) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                for (Iterator<FairScheduledRunnable> it = q.iterator(); it.hasNext(); ) {
                    if (o == it.next()) {
                        it.remove();
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Iterator<Runnable> iterator() {
            return new Itr(toArray());
        }

        private class Itr implements Iterator<Runnable> {
            final Object[] array; // Array of all elements
            int cursor;           // index of next element to return
            int lastRet;          // index of last element, or -1 if no such

            Itr(Object[] array) {
                lastRet = -1;
                this.array = array;
            }

            public boolean hasNext() {
                return cursor < array.length;
            }

            @SuppressWarnings("unchecked")
            public Runnable next() {
                if (cursor >= array.length)
                    throw new NoSuchElementException();
                lastRet = cursor;
                return (Runnable) array[cursor++];
            }

            public void remove() {
                if (lastRet < 0)
                    throw new IllegalStateException();
                removeEQ(array[lastRet]);
                lastRet = -1;
            }
        }
    }

    private static long now() {
        return System.nanoTime();
    }
}
