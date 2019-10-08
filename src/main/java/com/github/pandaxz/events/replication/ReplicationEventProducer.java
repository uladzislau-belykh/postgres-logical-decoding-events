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

import com.github.pandaxz.events.dto.json.ChangeEvent;
import com.google.gson.Gson;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Use {@link ReplicationStream} for receive events from PostgreSql replication stream and provide
 * it to {@link ReplicationEventHandler}. Guarantees "only once" producing.
 *
 * @author Uladzislau Belykh
 */
public class ReplicationEventProducer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationEventProducer.class);

    private ReplicationStream replicationStream;
    private ReplicationEventHandler replicationEventHandler;
    private ReplicationEventProducerStatisticHandler statisticHandler = new SimpleReplicationEventProducerStatisticHandler();

    private Gson gson = new Gson();
    private CompletableFuture producer;
    private volatile boolean producing = false;

    /**
     * Instantiates a new Replication event producer.
     *
     * @param replicationStream       the replication stream
     * @param replicationEventHandler the replication event handler
     */
    public ReplicationEventProducer(ReplicationStream replicationStream, ReplicationEventHandler replicationEventHandler) {
        Objects.requireNonNull(replicationEventHandler, "Replication event handler should be not null");
        Objects.requireNonNull(replicationStream, "Replication stream should be not null");

        this.replicationStream = replicationStream;
        this.replicationEventHandler = replicationEventHandler;
    }

    /**
     * Start producing with provided idle poll timeout in thread from default pool.
     *
     * @param idlePollTimeout the idlePollTimeout
     */
    public void start(Long idlePollTimeout) {
        this.start(idlePollTimeout, null);
    }

    /**
     * Start producing with provided idle poll timeout in thread from executor.
     *
     * @param idlePollTimeout  the idlePollTimeout
     * @param executor the executor
     */
    public void start(Long idlePollTimeout, Executor executor) {
        if (this.producer != null) {
            throw new RuntimeException("Producer is running");
        }
        this.producing = true;
        if (executor == null) {
            this.producer = CompletableFuture.runAsync(() -> this.run(idlePollTimeout));
        } else {
            this.producer = CompletableFuture.runAsync(() -> this.run(idlePollTimeout), executor);
        }
        statisticHandler.producerIsRunning();
    }

    /**
     * Check is producer work.
     *
     * @return the boolean
     */
    public boolean isProducing() {
        return producer != null;
    }

    /**
     * Stop producer.
     */
    public void stop() {
        if (producer == null || !producing) {
            return;
        }
        producing = false;
        producer.join();
        producer = null;
        statisticHandler.producerIsStopped();
    }

    /**
     * Drop replication slot. Return true, if drop slot successfully
     *
     * @return the boolean
     * @throws SQLException the sql exception
     */
    public boolean dropSlot() throws SQLException {
        if (producing) {
            throw new RuntimeException("Producer is active");
        }
        //todo add check slot is active by other app
        try {
            replicationStream.close();
        } catch (IOException ignored) {
        }
        return replicationStream.dropSlot();
    }

    /**
     * Create replication slot. Return true, if create slot successfully
     *
     * @return the boolean
     * @throws SQLException the sql exception
     */
    public boolean createSlot() throws SQLException {
        return replicationStream.createSlot();
    }

    /**
     * Check is slot exists.
     *
     * @return the boolean
     * @throws SQLException the sql exception
     */
    public boolean slotExists() throws SQLException {
        return replicationStream.slotExists();
    }

    /**
     * Gets replication stream.
     *
     * @return the replication stream
     */
    public ReplicationStream getReplicationStream() {
        return replicationStream;
    }

    /**
     * Sets replication stream.
     *
     * @param replicationStream the replication stream
     */
    public void setReplicationStream(ReplicationStream replicationStream) {
        this.replicationStream = replicationStream;
    }

    /**
     * Gets replication event handler.
     *
     * @return the replication event handler
     */
    public ReplicationEventHandler getReplicationEventHandler() {
        return replicationEventHandler;
    }

    /**
     * Sets replication event handler.
     *
     * @param replicationEventHandler the replication event handler
     */
    public void setReplicationEventHandler(ReplicationEventHandler replicationEventHandler) {
        this.replicationEventHandler = replicationEventHandler;
    }

    /**
     * Sets statistic handler.
     *
     * @param statisticHandler the statistic handler
     */
    public void setStatisticHandler(ReplicationEventProducerStatisticHandler statisticHandler) {
        Objects.requireNonNull(statisticHandler);
        this.statisticHandler = statisticHandler;
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    private void run(Long idlePollTimeout) {
        try {
            if (!slotExists()) {
                createSlot();
            }
        } catch (SQLException e) {
            logger.error("Cannot create or check slot cause ", e);
        }

        try {
            while (this.producing) {
                boolean isProduced = produce();
                if (!isProduced) {
                    Thread.sleep(idlePollTimeout);
                }
            }
        } catch (InterruptedException e) {
            this.producing = false;
            this.producer = null;
            logger.error("Producer was interrupted", e);
        }
    }

    private boolean produce() {
        ReplicationEvent replicationEvent = replicationStream.receive();
        if (replicationEvent == null) {
            logger.trace("Replication stream return null");
            return false;
        }

        try {
            statisticHandler.eventIsReceived();
            String message = replicationEvent.getMessage();
            ChangeEvent changes = null;
            if (shouldProcessMessage(message)) {
                changes = gson.fromJson(message, ChangeEvent.class);
                if (changes.getChanges().isEmpty()) {
                    return false;
                }
                replicationEventHandler.handle(changes);
                statisticHandler.eventIsHandled(changes, replicationEvent.getReadTime());
            }
            LogSequenceNumber nextLsn = getNextLsn(changes);
            replicationStream.commit(replicationEvent.getLastReceiveLSN(), nextLsn);
        } catch (Exception e) {
            logger.debug("Replication message handling throw error", e);
            replicationStream.resetUncommitted();
            return false;
        }
        return true;
    }

    private boolean shouldProcessMessage(String msg) {
        return !msg.matches("\\{[\\s\\S]*\"change\":\\s*\\[\\s*\\]\\s*\\}$");
    }

    private LogSequenceNumber getNextLsn(ChangeEvent changes) {
        LogSequenceNumber nextLsn = null;
        if (changes != null) {
            nextLsn = LogSequenceNumber.valueOf(changes.getNextLsn());
        }

        return nextLsn;
    }
}
