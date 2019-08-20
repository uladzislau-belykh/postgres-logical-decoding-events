package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public class EventQueueStatisticHandler {

    private String table;
    private int queueNumber;
    private EventHolderStatisticHandler statisticHandler;

    public EventQueueStatisticHandler(String table, int queueNumber, EventHolderStatisticHandler statisticHandler) {
        this.table = table;
        this.queueNumber = queueNumber;
        this.statisticHandler = statisticHandler;
    }

    public void eventAddedToQueue(Instant timestamp, Change<Map<String, String>> event) {
        statisticHandler.eventAddedToHolder(this.table, queueNumber, timestamp, event);
    }

    public void eventHandledByQueue(Instant timestamp, Change<Map<String, String>> event) {
        statisticHandler.eventHandledInHolder(this.table, queueNumber, timestamp, event);
    }

    public void eventHandled(String handlerName, Long duration, Change<Map<String, String>> event) {
        statisticHandler.eventHandled(this.table, queueNumber, handlerName, duration, event);
    }
}
