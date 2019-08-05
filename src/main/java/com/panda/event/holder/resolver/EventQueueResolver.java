package com.panda.event.holder.resolver;

import com.panda.event.dto.Change;

import java.util.Map;

public interface EventQueueResolver {
    int resolve(int queueCount, Change<Map<String, String>> event);
}
