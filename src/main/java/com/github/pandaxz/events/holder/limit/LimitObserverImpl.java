package com.github.pandaxz.events.holder.limit;

import com.github.pandaxz.events.holder.CountLatch;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitObserverImpl implements LimitObserver {

    private int queueLimit;
    private CountLatch countLatch;
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public LimitObserverImpl(int queueLimit, CountLatch countLatch) {
        this.queueLimit = queueLimit;
        this.countLatch = countLatch;
    }

    @Override
    public void add(){
        int value = count.incrementAndGet();
        if(value > queueLimit){
            boolean prevValueIsClosed = isClosed.getAndSet(true);
            if(!prevValueIsClosed){
                countLatch.countUp();
            }
        }
    }

    @Override
    public void delete(){
        int value = count.decrementAndGet();
        if(value < queueLimit){
            boolean prevValueIsClosed = isClosed.getAndSet(false);
            if(prevValueIsClosed){
                countLatch.countDown();
            }
        }
    }
}
