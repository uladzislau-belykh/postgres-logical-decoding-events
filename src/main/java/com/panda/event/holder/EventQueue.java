package com.panda.event.holder;

import com.panda.event.dto.Change;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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

    public EventQueue(Set<EventHandler> handlers, Executor executor) {
        Runnable eventHandler = () -> {
            try {
                while (this.isHandling) {
                    Change<Map<String, String>> change = this.eventQueue.poll();
                    if (change == null) {
                        Thread.sleep(1000L);
                        continue;
                    }
                    for (EventHandler handler : handlers) {
                        try {
                            handler.handle(change);
                        } catch (Exception e) {
                            this.logger.error("Error when handling event: " + change + " exception: ", e);
                        }
                    }
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
        this.poller.join();
    }
}
