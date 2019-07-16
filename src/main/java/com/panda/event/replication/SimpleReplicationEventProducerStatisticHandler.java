package com.panda.event.replication;

import com.panda.event.dto.json.ChangeEvent;

import java.time.Instant;

public class SimpleReplicationEventProducerStatisticHandler implements ReplicationEventProducerStatisticHandler {
    @Override
    public void producerIsRunning() {

    }

    @Override
    public void producerIsStopped() {

    }

    @Override
    public void eventIsReceived() {

    }

    @Override
    public void eventIsHandled(ChangeEvent changes, Instant readTime) {

    }
}
