package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public class SimpleEventHolderStatisticHandler implements EventHolderStatisticHandler {
    @Override
    public void eventAddedToQueue(String table, int queueNumber, String handlerName, Instant timestamp, Change<Map<String, String>> event) {

    }

    @Override
    public void eventHandled(String table, int queueNumber, String handlerName, Instant timestamp, Change<Map<String, String>> event) {

    }

    @Override
    public void eventHandled(String table, int queueNumber, String handlerName, Long timestamp, Change<Map<String, String>> event) {

    }

    @Override
    public void eventPolledFromQueue(String table, int queueNumber, String handlerName, Instant timestamp, Change<Map<String, String>> event) {

    }
}
