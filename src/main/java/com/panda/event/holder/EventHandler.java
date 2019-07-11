package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.util.Map;

public interface EventHandler {

    void handle(Change<Map<String, String>> event);
}
