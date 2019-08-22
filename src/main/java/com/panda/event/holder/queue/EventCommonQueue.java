package com.panda.event.holder.queue;

import com.panda.event.dto.Change;
import com.panda.event.holder.EventHandler;
import com.panda.event.holder.EventQueue;
import com.panda.event.holder.statistic.EventQueueStatisticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class EventCommonQueue implements EventQueue {
    private static final Logger logger = LoggerFactory.getLogger(EventCommonQueue.class);

    private volatile boolean isReceiving = true;
    private volatile boolean isHandling = true;
    private CompletableFuture poller;
    private Queue<Change<Map<String, String>>> eventQueue = new ConcurrentLinkedQueue<>();
    private EventQueueStatisticHandler statisticHandler;

    public EventCommonQueue(Set<EventHandler> handlers, Executor pollerExecutor, long idlePollPeriod, EventQueueStatisticHandler eventQueueStatisticHandler,
                            Executor handlerExecutor) {
        this.statisticHandler = eventQueueStatisticHandler;
        Runnable eventHandler = () -> {
            try {
                while (this.isHandling) {
                    Change<Map<String, String>> event = this.eventQueue.poll();
                    if (event == null) {
                        Thread.sleep(idlePollPeriod);
                        continue;
                    }
                    this.statisticHandler.eventPolledFromQueue(Instant.now(Clock.systemUTC()), event);
                    handle(handlers, event, handlerExecutor);
                    this.statisticHandler.eventHandled(Instant.now(Clock.systemUTC()), event);
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

    @Override
    public void add(Change<Map<String, String>> event) {
        if (!this.isReceiving) {
            throw new RuntimeException("Event queue stop work");
        }
        Instant addTimestamp = Instant.now(Clock.systemUTC());
        this.eventQueue.add(event);
        this.statisticHandler.eventAddedToQueue(addTimestamp, event);
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
        this.poller.join();
    }

    private void handle(Set<EventHandler> handlers, Change<Map<String, String>> event, Executor handlerExecutor) {
        CompletableFuture.allOf(handlers.stream()
                .map(handler -> {
                    Runnable task = () -> {
                        Long start = Clock.systemUTC().millis();
                        try {
                            handler.handle(event);
                        } catch (Exception e) {
                            logger.error("Error when handling event: " + event + " exception: ", e);
                        }
                        this.statisticHandler.eventHandled(handler.getClass().getSimpleName(), Clock.systemUTC().millis() - start, event);
                    };
                    return handlerExecutor == null ? CompletableFuture.runAsync(task) : CompletableFuture.runAsync(task, handlerExecutor);
                })
                .toArray(CompletableFuture[]::new))
                .join();
    }
}
