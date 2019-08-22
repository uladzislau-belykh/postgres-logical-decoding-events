package com.panda.event.holder;

import com.panda.event.dto.Change;

import java.io.Closeable;
import java.util.Map;

public interface EventQueue extends Closeable {

    void add(Change<Map<String, String>> event);
}
