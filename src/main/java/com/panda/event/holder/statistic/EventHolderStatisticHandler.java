package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public interface EventHolderStatisticHandler {
    void eventAddedToHolder(String table, int queueNumber, Instant timestamp, Change<Map<String, String>> event);

    void eventHandledInHolder(String table, int queueNumber, Instant timestamp, Change<Map<String, String>> event);

    void eventHandled(String table, int queueNumber, String handlerName, Long timestamp, Change<Map<String, String>> event);
}
