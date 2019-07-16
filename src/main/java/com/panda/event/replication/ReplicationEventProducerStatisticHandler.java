package com.panda.event.replication;

import com.panda.event.dto.json.ChangeEvent;

import java.time.Instant;

public interface ReplicationEventProducerStatisticHandler {

    void producerIsRunning();

    void producerIsStopped();

    void eventIsReceived();

    void eventIsHandled(ChangeEvent changes, Instant readTime);
}
