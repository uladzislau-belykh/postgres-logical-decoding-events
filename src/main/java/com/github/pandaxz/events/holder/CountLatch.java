package com.github.pandaxz.events.holder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author Uladzislau Belykh
 */
public class CountLatch {

    private class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1L;

        public Sync() {
        }

        @Override
        protected int tryAcquireShared(int arg) {
            return count.get() == releaseValue ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(int arg) {
            return true;
        }
    }

    private final Sync sync;
    private final AtomicLong count;
    private volatile long releaseValue;

    public CountLatch(final long initial, final long releaseValue) {
        this.releaseValue = releaseValue;
        this.count = new AtomicLong(initial);
        this.sync = new Sync();
    }

    public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public long countUp() {
        final long current = count.incrementAndGet();
        if (current == releaseValue) {
            sync.releaseShared(0);
        }
        return current;
    }

    public long countDown() {
        final long current = count.decrementAndGet();
        if (current == releaseValue) {
            sync.releaseShared(0);
        }
        return current;
    }

    public long getCount() {
        return count.get();
    }
}
