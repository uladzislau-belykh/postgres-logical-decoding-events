package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class EventHolder implements Closeable {

    private static final int DEFAULT_COUNT = 10;
    private Map<String, EventQueueHolder> holders;
    private volatile boolean isReceiving = true;
    private volatile Semaphore semaphore = new Semaphore(DEFAULT_COUNT);

    public EventHolder() {
        this.holders = new HashMap<>();
    }

    public void add(List<Change<Map<String, String>>> events) {
        if (!isReceiving) {
            throw new RuntimeException("Data holder stop work");
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
        }
        for (Change<Map<String, String>> event : events) {
            EventQueueHolder eventQueueHolder = holders.get(event.getTable());
            if (eventQueueHolder != null) {
                eventQueueHolder.add(event);
            }
        }
        semaphore.release();
    }

    public void registerHolder(String table, int queueCount, EventQueueResolver resolver) {
        holders.put(table, new EventQueueHolder(table, queueCount, resolver));
    }

    public void registerHolder(String table, int queueCount, EventQueueResolver resolver, Executor executor) {
        holders.put(table, new EventQueueHolder(table, queueCount, resolver, executor));
    }

    public void unregisterHolder(String table) {
        holders.remove(table);
    }

    public void registerHandler(String table, EventHandler handler) {
        holders.getOrDefault(table, new EventQueueHolder(table, 1, new SimpleEventQueueResolver()))
                .registerHandler(handler);
    }

    public void unregisterHandler(String table, EventHandler handler) {
        EventQueueHolder eventQueueHolder = holders.get(table);
        if (eventQueueHolder != null) {
            eventQueueHolder.unregisterHandler(handler);
        }
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
