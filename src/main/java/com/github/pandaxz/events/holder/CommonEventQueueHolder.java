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
import com.github.pandaxz.events.holder.statistic.EventHolderStatisticHandler;
import com.github.pandaxz.events.holder.statistic.EventQueueStatisticHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @author Uladzislau Belykh
 */
public class CommonEventQueueHolder implements EventQueueHolder {

    private String table;
    private List<EventQueue> queues;
    private Set<EventHandler> handlers;
    private EventQueueResolver resolver;
    private Executor pollerExecutor;
    private Executor handlerExecutor;
    private Integer queueCount;

    public CommonEventQueueHolder(String table, int queueCount, EventQueueResolver resolver) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.queueCount = queueCount;
    }

    public CommonEventQueueHolder(String table, int queueCount, EventQueueResolver resolver, Executor pollerExecutor, Executor handlerExecutor) {
        this.table = table;
        this.handlers = new HashSet<>();
        this.resolver = resolver;
        this.pollerExecutor = pollerExecutor;
        this.handlerExecutor = handlerExecutor;
        this.queueCount = queueCount;
    }

    @Override
    public void init(EventHolderStatisticHandler statisticHandler) {
        if (this.queues == null) {
            this.queues = new ArrayList<>();
            for (int i = 0; i < this.queueCount; i++) {
                EventQueueStatisticHandler eventQueueStatisticHandler = new EventQueueStatisticHandler(this.table, i, statisticHandler, null);
                EventQueue queue = new EventQueue(this.handlers, this.pollerExecutor, eventQueueStatisticHandler, this.handlerExecutor);
                this.queues.add(queue);
            }
        }
    }

    @Override
    public void add(Change<Map<String, String>> event) {
        EventQueue eventQueue = getEventQueue(event);
        eventQueue.add(event);
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
