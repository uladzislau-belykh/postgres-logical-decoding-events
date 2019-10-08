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

package com.github.pandaxz.events.replication;

import org.postgresql.replication.LogSequenceNumber;

import java.time.Instant;

/**
 * Entity that reproduce data received from replication stream. In most cases reproduce transaction in database.
 *
 * @author Uladzislau Belykh
 */
public class ReplicationEvent {
        private Instant readTime;
        private LogSequenceNumber lastReceiveLSN;
        private String message;

    /**
     * Instantiates a new Replication event.
     *
     * @param msg            the msg
     * @param readTime       the read time
     * @param lastReceiveLSN the last receive lsn
     */
    public ReplicationEvent(String msg, Instant readTime, LogSequenceNumber lastReceiveLSN) {
        this.readTime = readTime;
        this.lastReceiveLSN = lastReceiveLSN;
        this.message = msg;
    }

    /**
     * Gets read time.
     *
     * @return the read time
     */
    public Instant getReadTime() {
        return readTime;
    }

    /**
     * Sets read time.
     *
     * @param readTime the read time
     */
    public void setReadTime(Instant readTime) {
        this.readTime = readTime;
    }

    /**
     * Gets last receive lsn.
     *
     * @return the last receive lsn
     */
    public LogSequenceNumber getLastReceiveLSN() {
        return lastReceiveLSN;
    }

    /**
     * Sets last receive lsn.
     *
     * @param lastReceiveLSN the last receive lsn
     */
    public void setLastReceiveLSN(LogSequenceNumber lastReceiveLSN) {
        this.lastReceiveLSN = lastReceiveLSN;
    }

    /**
     * Gets message.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets message.
     *
     * @param message the message
     */
    public void setMessage(String message) {
        this.message = message;
    }
}
