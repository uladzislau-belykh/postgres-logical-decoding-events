package com.panda.event.holder;

import com.panda.event.dto.Change;
import com.panda.event.holder.resolver.EventQueueResolver;
import com.panda.event.holder.statistic.EventHolderStatisticHandler;
import com.panda.event.holder.statistic.EventQueueStatisticHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public class DistributedEventQueueHolder implements EventQueueHolder {

    private String table;
    private List<Set<EventQueue>> queues;
    private Set<EventHandler> handlers;
    private EventQueueResolver resolver;
    private Executor pollerExecutor;
    private Integer queueCount;

    public DistributedEventQueueHolder(String table, int queueCount, EventQueueResolver resolver) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.queueCount = queueCount;
    }

    public DistributedEventQueueHolder(String table, int queueCount, EventQueueResolver resolver, Executor pollerExecutor) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.pollerExecutor = pollerExecutor;
        this.queueCount = queueCount;
    }

    @Override
    public void init(EventHolderStatisticHandler statisticHandler, Long idlePollPeriod) {
        if (this.queues == null) {
            this.queues = new ArrayList<>();
            for (int i = 0; i < this.queueCount; i++) {
                Set<EventQueue> queues = new HashSet<>();
                this.queues.add(queues);
                for (EventHandler handler : this.handlers) {
                    EventQueueStatisticHandler eventQueueStatisticHandler = new EventQueueStatisticHandler(this.table, i, statisticHandler,
                            handler.getClass().getSimpleName());
                    EventQueue queue = new EventQueue(handler, this.pollerExecutor, idlePollPeriod, eventQueueStatisticHandler);
                    queues.add(queue);
                }
            }
        }
    }

    @Override
    public void add(Change<Map<String, String>> event) {
        Set<EventQueue> eventQueues = getEventQueue(event);
        eventQueues.forEach(queue -> queue.add(event));
    }

    @Override
    public void registerHandler(EventHandler handler) {
        this.handlers.add(handler);
    }

    @Override
    public void unregisterHandler(EventHandler handler) {
        this.handlers.remove(handler);
    }

    @Override
    public void close() throws IOException {
        if (this.queues != null) {
            for (Set<EventQueue> queue : this.queues) {
                for (EventQueue eventQueue : queue) {
                    eventQueue.close();
                }
            }
            this.queues = null;
        }
    }

    private Set<EventQueue> getEventQueue(Change<Map<String, String>> event) {
        int resolve = this.resolver.resolve(this.queueCount, event);
        return this.queues.get(resolve);
    }
}
