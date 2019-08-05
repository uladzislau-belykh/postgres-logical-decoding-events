package com.panda.event.holder.resolver;

import com.panda.event.dto.Change;

import java.util.Map;

public class SimpleEventQueueResolver implements EventQueueResolver {
    @Override
    public int resolve(int queueCount, Change<Map<String, String>> event) {
        return 0;
    }
}
