package com.panda.event.replication;

import com.google.gson.Gson;
import com.panda.event.holder.EventHolder;
import com.panda.event.dto.Change;
import com.panda.event.dto.ChangeType;
import com.panda.event.dto.json.ChangeEvent;
import com.panda.event.dto.json.ChangeMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleReplicationEventHandler implements ReplicationEventHandler{

    private EventHolder eventsHolder;

    private Gson gson = new Gson();

    public SimpleReplicationEventHandler(EventHolder eventsHolder) {
        this.eventsHolder = eventsHolder;
    }

    public SimpleReplicationEventHandler() {
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

    public EventHolder getEventsHolder() {
        return eventsHolder;
    }

    public void setEventsHolder(EventHolder eventsHolder) {
        this.eventsHolder = eventsHolder;
    }
}