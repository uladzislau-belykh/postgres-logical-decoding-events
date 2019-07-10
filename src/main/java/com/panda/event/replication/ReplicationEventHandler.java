package com.panda.event.replication;

import com.panda.event.replication.dto.json.ChangeEvent;

public interface ReplicationEventHandler {

    void handle(ChangeEvent changeEvent);

}
