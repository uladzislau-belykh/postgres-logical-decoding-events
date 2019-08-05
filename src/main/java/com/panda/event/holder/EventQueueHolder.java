package com.panda.event.holder;

import com.panda.event.dto.Change;
import com.panda.event.holder.resolver.EventQueueResolver;
import com.panda.event.holder.statistic.EventHolderStatisticHandler;
import com.panda.event.holder.statistic.EventQueueStatisticHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public class EventQueueHolder implements Closeable {

    private String table;
    private List<EventQueue> queues;
    private Set<EventHandler> handlers;
    private EventQueueResolver resolver;
    private Executor executor;
    private Integer queueCount;

    public EventQueueHolder(String table, int queueCount, EventQueueResolver resolver) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.queueCount = queueCount;
    }

    public EventQueueHolder(String table, int queueCount, EventQueueResolver resolver, Executor executor) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.executor = executor;
        this.queueCount = queueCount;
    }

    public void init(EventHolderStatisticHandler statisticHandler) {
        if (queues == null) {
            this.queues = new ArrayList<>();
            for (int i = 0; i < queueCount; i++) {
                EventQueueStatisticHandler eventQueueStatisticHandler = createEventQueueStatisticHandler(statisticHandler, i);
                queues.add(new EventQueue(handlers, executor, eventQueueStatisticHandler));
            }
        }
    }

    public void add(Change<Map<String, String>> event) {
        EventQueue eventQueue = getEventQueue(event);
        eventQueue.add(event);
    }

    public void registerHandler(EventHandler handler) {
        handlers.add(handler);
    }

    public void unregisterHandler(EventHandler handler) {
        handlers.remove(handler);
    }

    @Override
    public void close() throws IOException {
        if (queues != null) {
            for (EventQueue queue : queues) {
                queue.close();
            }
            queues = null;
        }
    }

    private EventQueue getEventQueue(Change<Map<String, String>> event) {
        int resolve = resolver.resolve(queueCount, event);
        return queues.get(resolve);
    }

    private EventQueueStatisticHandler createEventQueueStatisticHandler(EventHolderStatisticHandler statisticHandler, int i) {
        return new EventQueueStatisticHandler((timestamp, event) -> statisticHandler.eventAddedToHolder(this.table, i, timestamp, event),
                (timestamp, event) -> statisticHandler.eventHandledInHolder(this.table, i, timestamp, event));
    }
}
