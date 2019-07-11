package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.util.Map;

public interface EventQueueResolver {
    int resolve(int queueCount, String table, Change<Map<String, String>> event);
}
