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
    private Executor pollerExecutor;
    private Executor handlerExecutor;
    private Integer queueCount;

    public EventQueueHolder(String table, int queueCount, EventQueueResolver resolver) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.queueCount = queueCount;
    }

    public EventQueueHolder(String table, int queueCount, EventQueueResolver resolver, Executor pollerExecutor, Executor handlerExecutor) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.pollerExecutor = pollerExecutor;
        this.handlerExecutor = handlerExecutor;
        this.queueCount = queueCount;
    }

    public void init(EventHolderStatisticHandler statisticHandler, Long idlePollPeriod) {
        if (this.queues == null) {
            this.queues = new ArrayList<>();
            for (int i = 0; i < this.queueCount; i++) {
                EventQueueStatisticHandler eventQueueStatisticHandler = new EventQueueStatisticHandler(this.table, i, statisticHandler);
                EventQueue queue = new EventQueue(this.handlers, this.pollerExecutor, eventQueueStatisticHandler, this.handlerExecutor);
                if (idlePollPeriod != null) {
                    queue.setIdlePollPeriod(idlePollPeriod);
                }
                this.queues.add(queue);
            }
        }
    }

    public void add(Change<Map<String, String>> event) {
        EventQueue eventQueue = getEventQueue(event);
        eventQueue.add(event);
    }

    public void registerHandler(EventHandler handler) {
        this.handlers.add(handler);
    }

    public void unregisterHandler(EventHandler handler) {
        this.handlers.remove(handler);
    }

    @Override
    public void close() throws IOException {
        if (this.queues != null) {
            for (EventQueue queue : this.queues) {
                queue.close();
            }
            this.queues = null;
        }
    }

    private EventQueue getEventQueue(Change<Map<String, String>> event) {
        int resolve = this.resolver.resolve(this.queueCount, event);
        return this.queues.get(resolve);
    }
}
