/*
 *   Copyright 2019 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.github.pandaxz.events.holder;

import com.github.pandaxz.events.dto.Change;
import com.github.pandaxz.events.holder.resolver.EventQueueResolver;
import com.github.pandaxz.events.holder.resolver.SimpleEventQueueResolver;
import com.github.pandaxz.events.holder.statistic.EventHolderStatisticHandler;
import com.github.pandaxz.events.holder.statistic.SimpleEventHolderStatisticHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author Uladzislau Belykh
 */
public class EventHolder implements Closeable {

    private static final int DEFAULT_COUNT = 10;
    private Map<String, EventQueueHolder> holders;
    private long delay = 30;
    private volatile boolean isReceiving = true;
    private final Semaphore semaphore = new Semaphore(DEFAULT_COUNT);
    private EventHolderStatisticHandler statisticHandler = new SimpleEventHolderStatisticHandler();
    private CountLatch countLatch = new CountLatch(0, 0);

    public EventHolder() {
        this.holders = new HashMap<>();
    }

    public boolean add(List<Change<Map<String, String>>> events) {
        if (!isReceiving) {
            throw new RuntimeException("Data holder stop work");
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
        }
        try {
            if (!countLatch.await(delay, TimeUnit.SECONDS)) {
                return false;
            }
            for (Change<Map<String, String>> event : events) {
                EventQueueHolder eventQueueHolder = holders.get(event.getTable());
                if (eventQueueHolder != null) {
                    eventQueueHolder.add(event);
                }
            }
            return true;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            semaphore.release();
        }
    }

    public void init() {
        for (EventQueueHolder value : this.holders.values()) {
            value.init(this.statisticHandler);
        }
    }

    public void registerCommonHolder(String table, int queueCount, EventQueueResolver resolver, int queueLimit) {
        if (queueLimit < 0 || queueCount < 0) {
            throw new IllegalArgumentException();
        }
        holders.put(table, new CommonEventQueueHolder(table, queueCount, resolver, queueLimit, countLatch));
    }

    public void registerCommonHolder(String table, int queueCount, EventQueueResolver resolver, int queueLimit, Executor pollerExecutor,
                                     Executor handlerExecutor) {
        if (queueLimit < 0 || queueCount < 0) {
            throw new IllegalArgumentException();
        }
        holders.put(table, new CommonEventQueueHolder(table, queueCount, resolver, queueLimit, countLatch, pollerExecutor, handlerExecutor));
    }

    public void registerDistributedHolder(String table, int queueCount, EventQueueResolver resolver, int queueLimit) {
        if (queueLimit < 0 || queueCount < 0) {
            throw new IllegalArgumentException();
        }
        holders.put(table, new DistributedEventQueueHolder(table, queueCount, resolver, queueLimit, countLatch));
    }

    public void registerDistributedHolder(String table, int queueCount, EventQueueResolver resolver, int queueLimit, Executor pollerExecutor) {
        if (queueLimit < 0 || queueCount < 0) {
            throw new IllegalArgumentException();
        }
        holders.put(table, new DistributedEventQueueHolder(table, queueCount, resolver, queueLimit, countLatch, pollerExecutor));
    }

    public void unregisterHolder(String table) {
        holders.remove(table);
    }

    public void registerHandler(String table, EventHandler handler) {
        holders.putIfAbsent(table, new CommonEventQueueHolder(table, 1, new SimpleEventQueueResolver(), countLatch));
        holders.get(table).registerHandler(handler);
    }

    public void unregisterHandler(String table, EventHandler handler) {
        EventQueueHolder eventQueueHolder = holders.get(table);
        if (eventQueueHolder != null) {
            eventQueueHolder.unregisterHandler(handler);
        }
    }

    public void setStatisticHandler(EventHolderStatisticHandler statisticHandler) {
        Objects.requireNonNull(statisticHandler);
        this.statisticHandler = statisticHandler;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    @Override
    public void close() throws IOException {
        this.isReceiving = false;
        semaphore.acquireUninterruptibly(DEFAULT_COUNT);
        for (EventQueueHolder holder : holders.values()) {
            holder.close();
        }
    }
}
