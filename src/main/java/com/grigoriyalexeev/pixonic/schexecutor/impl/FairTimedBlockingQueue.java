package com.grigoriyalexeev.pixonic.schexecutor.impl;

import com.grigoriyalexeev.pixonic.schexecutor.FairScheduledRunnable;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.grigoriyalexeev.pixonic.schexecutor.impl.Utils.nowMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FairTimedBlockingQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {
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
            if (first == null || nowMillis() < first.getTime(TimeUnit.MILLISECONDS))
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
                    long time = first.getTime(MILLISECONDS);
                    if (nowMillis() >= time)
                        return q.poll();
                    first = null; // don't retain ref while waiting
                    if (leader != null)
                        available.await();
                    else {
                        Thread thisThread = Thread.currentThread();
                        leader = thisThread;
                        try {
                            available.awaitNanos(nowMillis() - time);
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
                    long scheduledTime = first.getTime(MILLISECONDS);
                    if (scheduledTime <= nowMillis())
                        return q.poll();
                    if (nanos <= 0)
                        return null;
                    first = null; // don't retain ref while waiting
                    if (nanos < (System.nanoTime() - scheduledTime * 1000) || leader != null)
                        nanos = available.awaitNanos(nanos);
                    else {
                        Thread thisThread = Thread.currentThread();
                        leader = thisThread;
                        try {
                            long delay = System.nanoTime() - scheduledTime * 1000;
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
        return (first == null || nowMillis() > first.getTime(MILLISECONDS)) ?
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

    private void removeEQ(Object o) {
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
