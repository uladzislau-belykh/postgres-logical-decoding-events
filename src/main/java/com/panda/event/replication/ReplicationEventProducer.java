package com.panda.event.replication;

import com.google.gson.Gson;
import com.panda.event.dto.json.ChangeEvent;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ReplicationEventProducer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationEventProducer.class);

    private ReplicationStream replicationStream;
    private ReplicationEventHandler replicationEventHandler;
    private ReplicationEventProducerStatisticHandler statisticHandler = new SimpleReplicationEventProducerStatisticHandler();

    private Gson gson = new Gson();
    private CompletableFuture producer;
    private volatile boolean producing = false;

    public ReplicationEventProducer(ReplicationStream replicationStream, ReplicationEventHandler replicationEventHandler) {
        Objects.requireNonNull(replicationEventHandler, "Replication event handler should be not null");
        Objects.requireNonNull(replicationStream, "Replication stream should be not null");

        this.replicationStream = replicationStream;
        this.replicationEventHandler = replicationEventHandler;
    }

    public void start(Long timeout, ExecutorService executorService) {
        if (this.producer != null) {
            throw new RuntimeException("Producer is running");
        }
        this.producing = true;
        this.producer = CompletableFuture.runAsync(() -> {
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
                        Thread.sleep(timeout);
                    }
                }
            } catch (InterruptedException e) {
                this.producing = false;
                this.producer = null;
                logger.error("Producer was interrupted", e);
            }
        });
        statisticHandler.producerIsRunning();
    }

    public void stop() {
        if (producer == null || !producing) {
            return;
        }
        producing = false;
        producer.join();
        producer = null;
        statisticHandler.producerIsStopped();
    }

    public boolean produce() {
        ReplicationEvent replicationEvent = replicationStream.receive();
        if (replicationEvent == null) {
            logger.debug("Replication stream return null");
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
            replicationStream.resetUncomitted();
            return false;
        }
        return true;
    }

    public boolean dropSlot() throws SQLException {
        if(producing){
            throw new RuntimeException("Producer is active");
        }
        //todo add check slot is active by other app
        try {
            replicationStream.close();
        } catch (IOException e) {}
        return replicationStream.dropSlot();
    }

    public boolean createSlot() throws SQLException {
        return replicationStream.createSlot();
    }

    public boolean slotExists() throws SQLException {
        return replicationStream.slotExists();
    }

    public ReplicationStream getReplicationStream() {
        return replicationStream;
    }

    public void setReplicationStream(ReplicationStream replicationStream) {
        this.replicationStream = replicationStream;
    }

    public ReplicationEventHandler getReplicationEventHandler() {
        return replicationEventHandler;
    }

    public void setReplicationEventHandler(ReplicationEventHandler replicationEventHandler) {
        this.replicationEventHandler = replicationEventHandler;
    }

    public void setStatisticHandler(ReplicationEventProducerStatisticHandler statisticHandler) {
        Objects.requireNonNull(statisticHandler);
        this.statisticHandler = statisticHandler;
    }

    @Override
    public void close() throws IOException {
        stop();
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
