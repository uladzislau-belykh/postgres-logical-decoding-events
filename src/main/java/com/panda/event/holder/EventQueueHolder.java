package com.panda.event.holder;

import com.panda.event.dto.Change;
import com.panda.event.holder.statistic.EventHolderStatisticHandler;

import java.io.Closeable;
import java.util.Map;

public interface EventQueueHolder extends Closeable {
    void init(EventHolderStatisticHandler statisticHandler, Long idlePollPeriod);

    void add(Change<Map<String, String>> event);

    void registerHandler(EventHandler handler);

    void unregisterHandler(EventHandler handler);
}
