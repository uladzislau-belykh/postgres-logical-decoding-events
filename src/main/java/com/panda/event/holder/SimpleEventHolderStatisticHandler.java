package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public class SimpleEventHolderStatisticHandler implements EventHolderStatisticHandler {
    @Override
    public void eventAddedToHolder(String table, int queueNumber, Instant timestamp, Change<Map<String, String>> event) {

    }

    @Override
    public void eventHandledInHolder(String table, int queueNumber, Instant timestamp, Change<Map<String, String>> event) {

    }
}
