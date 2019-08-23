package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public class EventQueueStatisticHandler {

    private String table;
    private int queueNumber;
    private EventHolderStatisticHandler statisticHandler;
    private String handlerName;

    public EventQueueStatisticHandler(String table, int queueNumber, EventHolderStatisticHandler statisticHandler, String handlerName) {
        this.table = table;
        this.queueNumber = queueNumber;
        this.statisticHandler = statisticHandler;
        this.handlerName = handlerName;
    }

    public void eventAddedToQueue(Instant timestamp, Change<Map<String, String>> event) {
        this.statisticHandler.eventAddedToHolder(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventPolledFromQueue(Instant timestamp, Change<Map<String, String>> event){
        this.statisticHandler.eventPolledFromQueue(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventHandled(Instant timestamp, Change<Map<String, String>> event) {
        this.statisticHandler.eventHandled(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventHandled(String handlerName, Long duration, Change<Map<String, String>> event) {
        this.statisticHandler.eventHandled(this.table, this.queueNumber, handlerName, duration, event);
    }
}
