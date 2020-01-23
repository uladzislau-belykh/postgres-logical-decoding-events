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
import com.github.pandaxz.events.holder.limit.LimitObserver;
import com.github.pandaxz.events.holder.limit.LimitObserverImpl;
import com.github.pandaxz.events.holder.limit.NoLimitObserverImpl;
import com.github.pandaxz.events.holder.statistic.EventQueueStatisticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Uladzislau Belykh
 */
public class EventQueue implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(EventQueue.class);

    private volatile boolean isReceiving = true;
    private volatile boolean isHandling = true;
    private CompletableFuture poller;
    private BlockingQueue<Change<Map<String, String>>> eventQueue = new LinkedBlockingQueue<>();
    private EventQueueStatisticHandler statisticHandler;
    private LimitObserver limitObserver;

    public EventQueue(EventHandler handler, Executor pollerExecutor, EventQueueStatisticHandler eventQueueStatisticHandler, int queueLimit,
                      CountLatch countLatch) {
        if(queueLimit > 0) {
            this.limitObserver = new LimitObserverImpl(queueLimit, countLatch);
        }else{
            this.limitObserver = new NoLimitObserverImpl();
        }
        this.statisticHandler = eventQueueStatisticHandler;
        Runnable eventHandler = () -> {
            try {
                while (this.isHandling) {
                    Change<Map<String, String>> event = this.eventQueue.take();
                    this.statisticHandler.eventPolledFromQueue(Instant.now(Clock.systemUTC()), event);
                    handleEvent(event, handler);
                    this.statisticHandler.eventHandled(Instant.now(Clock.systemUTC()), event);
                    this.limitObserver.delete();
                }
            } catch (Exception e) {
            }
        };

        if (pollerExecutor == null) {
            this.poller = CompletableFuture.runAsync(eventHandler);
        } else {
            this.poller = CompletableFuture.runAsync(eventHandler, pollerExecutor);
        }
    }

    public EventQueue(Set<EventHandler> handlers, Executor pollerExecutor, EventQueueStatisticHandler eventQueueStatisticHandler, int queueLimit,
                      CountLatch countLatch, Executor handlerExecutor) {
        if(queueLimit > 0) {
            this.limitObserver = new LimitObserverImpl(queueLimit, countLatch);
        }else{
            this.limitObserver = new NoLimitObserverImpl();
        }
        this.statisticHandler = eventQueueStatisticHandler;
        Runnable eventHandler = () -> {
            try {
                while (this.isHandling) {
                    Change<Map<String, String>> event = this.eventQueue.take();
                    this.statisticHandler.eventPolledFromQueue(Instant.now(Clock.systemUTC()), event);
                    handle(handlers, event, handlerExecutor);
                    this.statisticHandler.eventHandled(Instant.now(Clock.systemUTC()), event);
                    this.limitObserver.delete();
                }
            } catch (Exception e) {
            }
        };

        if (pollerExecutor == null) {
            this.poller = CompletableFuture.runAsync(eventHandler);
        } else {
            this.poller = CompletableFuture.runAsync(eventHandler, pollerExecutor);
        }
    }

    public void add(Change<Map<String, String>> event) {
        if (!this.isReceiving) {
            throw new RuntimeException("Event queue stop work");
        }
        Instant addTimestamp = Instant.now(Clock.systemUTC());
        this.statisticHandler.eventAddedToQueue(addTimestamp, event);
        this.limitObserver.add();
        this.eventQueue.add(event);
    }

    @Override
    public void close() throws IOException {
        this.isReceiving = false;
        while (!eventQueue.isEmpty()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                logger.info("Event queue is interrupted, probably you lost events");
            }
        }
        this.isHandling = false;
    }

    private void handle(Set<EventHandler> handlers, Change<Map<String, String>> event, Executor handlerExecutor) {
        CompletableFuture.allOf(handlers.stream()
                .map(handler -> {
                    Runnable task = () -> handleEvent(event, handler);
                    return handlerExecutor == null ? CompletableFuture.runAsync(task) : CompletableFuture.runAsync(task, handlerExecutor);
                })
                .toArray(CompletableFuture[]::new))
                .join();
    }

    private void handleEvent(Change<Map<String, String>> event, EventHandler handler) {
        long start = Clock.systemUTC().millis();
        try {
            handler.handle(event);
        } catch (Exception e) {
            logger.error("Error when handling event: " + event + " exception: ", e);
        }
        this.statisticHandler.eventHandled(handler.getHandlerName(), Clock.systemUTC().millis() - start, event);
    }
}
