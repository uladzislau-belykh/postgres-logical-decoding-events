package com.panda.event.replication;

import org.postgresql.replication.LogSequenceNumber;

import java.time.Instant;

public class ReplicationEvent {
        private Instant readTime;
        private LogSequenceNumber lastReceiveLSN;
        private String message;

    public ReplicationEvent(String msg, Instant readTime, LogSequenceNumber lastReceiveLSN) {
        this.readTime = readTime;
        this.lastReceiveLSN = lastReceiveLSN;
        this.message = message;
    }

    public Instant getReadTime() {
        return readTime;
    }

    public void setReadTime(Instant readTime) {
        this.readTime = readTime;
    }

    public LogSequenceNumber getLastReceiveLSN() {
        return lastReceiveLSN;
    }

    public void setLastReceiveLSN(LogSequenceNumber lastReceiveLSN) {
        this.lastReceiveLSN = lastReceiveLSN;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
