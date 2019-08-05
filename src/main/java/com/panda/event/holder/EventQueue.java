package com.panda.event.holder;

import com.panda.event.dto.Change;
import com.panda.event.holder.statistic.EventQueueStatisticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class EventQueue implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(EventQueue.class);

    private volatile boolean isReceiving = true;
    private volatile boolean isHandling = true;
    private CompletableFuture poller;
    private Queue<Change<Map<String, String>>> eventQueue = new ConcurrentLinkedQueue<>();
    private EventQueueStatisticHandler statisticHandler;

    public EventQueue(Set<EventHandler> handlers, Executor executor, EventQueueStatisticHandler eventQueueStatisticHandler) {
        this.statisticHandler = eventQueueStatisticHandler;
        Runnable eventHandler = () -> {
            try {
                while (this.isHandling) {
                    Change<Map<String, String>> event = this.eventQueue.poll();
                    if (event == null) {
                        Thread.sleep(1000L);
                        continue;
                    }
                    for (EventHandler handler : handlers) {
                        try {
                            handler.handle(event);
                        } catch (Exception e) {
                            logger.error("Error when handling event: " + event + " exception: ", e);
                        }
                    }
                    this.statisticHandler.eventHandledByQueue(Instant.now(Clock.systemUTC()), event);
                }
            } catch (Exception e) {
            }
        };

        if (executor == null) {
            this.poller = CompletableFuture.runAsync(eventHandler);
        } else {
            this.poller = CompletableFuture.runAsync(eventHandler, executor);
        }
    }

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
}
