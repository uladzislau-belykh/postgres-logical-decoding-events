package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.util.Map;

public class SimpleEventQueueResolver implements EventQueueResolver{
    @Override
    public int resolve(int queueCount, String table, Change<Map<String, String>> event) {
        return 0;
    }
}
