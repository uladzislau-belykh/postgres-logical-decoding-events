/*
 *   Copyright 2019 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.panda.event.replication;

import com.google.gson.Gson;
import com.panda.event.dto.Change;
import com.panda.event.dto.ChangeType;
import com.panda.event.dto.json.ChangeEvent;
import com.panda.event.dto.json.ChangeMessage;
import com.panda.event.holder.EventHolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link ReplicationEventHandler} implementation that convert entity and provide it to {@link EventHolder}.
 *
 * @author Uladzislau Belykh
 */
public class SimpleReplicationEventHandler implements ReplicationEventHandler {

    private EventHolder eventsHolder;

    private Gson gson = new Gson();

    /**
     * Instantiates a new Simple replication event handler.
     *
     * @param eventsHolder the events holder
     */
    public SimpleReplicationEventHandler(EventHolder eventsHolder) {
        this.eventsHolder = eventsHolder;
    }

    @Override
    public void handle(ChangeEvent changeEvent) {
        eventsHolder.add(changeEvent.getChanges().stream().map(this::convertToChangeMap).collect(Collectors.toList()));
    }

    private Change<Map<String, String>> convertToChangeMap(ChangeMessage changeMessage) {
        Change<Map<String, String>> change = new Change<>();
        change.setTable(changeMessage.getTable());
        change.setType(ChangeType.valueOf(changeMessage.getKind().toUpperCase()));

        if (changeMessage.getColumnNames() != null && changeMessage.getColumnValues() != null) {
            change.setNewValus(convertToMap(changeMessage.getColumnNames(), changeMessage.getColumnValues()));
        }

        if (changeMessage.getOldKeys() != null) {
            change.setOldValue(convertToMap(changeMessage.getOldKeys().getKeyNames(), changeMessage.getOldKeys().getKeyValues()));
        }
        return change;
    }

    private Map<String, String> convertToMap(List<String> keyNames, List<String> keyValues) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < keyNames.size(); i++) {
            String name = keyNames.get(i);
            String value = keyValues.get(i);
            result.put(name, value);
        }

        return result;
    }

    /**
     * Gets events holder.
     *
     * @return the events holder
     */
    public EventHolder getEventsHolder() {
        return eventsHolder;
    }

    /**
     * Sets events holder.
     *
     * @param eventsHolder the events holder
     */
    public void setEventsHolder(EventHolder eventsHolder) {
        this.eventsHolder = eventsHolder;
    }
}
