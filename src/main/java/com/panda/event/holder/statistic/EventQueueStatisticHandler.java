package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;
import java.util.function.BiConsumer;

public class EventQueueStatisticHandler {

    private BiConsumer<Instant, Change<Map<String, String>>> eventAddedToQueue;
    private BiConsumer<Instant, Change<Map<String, String>>> eventHandledByQueue;

    public EventQueueStatisticHandler(BiConsumer<Instant, Change<Map<String, String>>> eventAddedToQueue,
                                      BiConsumer<Instant, Change<Map<String, String>>> eventHandledByQueue) {
        this.eventAddedToQueue = eventAddedToQueue;
        this.eventHandledByQueue = eventHandledByQueue;
    }

    public void eventAddedToQueue(Instant timestamp, Change<Map<String, String>> event) {
        eventAddedToQueue.accept(timestamp, event);
    }

    public void eventHandledByQueue(Instant timestamp, Change<Map<String, String>> event) {
        eventHandledByQueue.accept(timestamp, event);
    }
}
