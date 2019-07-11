package com.panda.event.replication;

import com.google.gson.Gson;
import com.panda.event.dto.json.ChangeEvent;
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
        if (producer != null) {
            throw new RuntimeException("Producer is running");
        }
        producing = true;
        CompletableFuture.runAsync(() -> {
            try {
                if (!slotExists()) {
                    createSlot();
                }
            } catch (SQLException e) {
                logger.error("Cannot create or check slot cause ", e);
            }

            try {
                while (producing) {
                    boolean isProduced = produce();
                    if (!isProduced) {
                        Thread.sleep(timeout);
                    }
                }
            }catch (InterruptedException e){
                producing = false;
                producer = null;
                logger.error("Producer was interrupted", e);
            }
        });
    }

    public void stop() {
        if (producer == null || !producing) {
            return;
        }
        producing = false;
        producer.join();
        producer = null;
    }

    //todo add statistic and metric handler
    public boolean produce() {
        ReplicationEvent replicationEvent = replicationStream.receive();
        if (replicationEvent == null) {
            logger.debug("Replication stream return null");
            return false;
        }

        try {
            String message = replicationEvent.getMessage();
            if (shouldProcessMessage(message)) {
                ChangeEvent changes = gson.fromJson(message, ChangeEvent.class);
                if (changes.getChanges().isEmpty()) {
                    return false;
                }
                replicationEventHandler.handle(changes);
            }
            replicationStream.commit(replicationEvent.getLastReceiveLSN());
        } catch (Exception e) {
            logger.debug("Replication message handling throw error", e);
            replicationStream.resetUncomitted();
            return false;
        }
        return true;
    }

    public boolean dropSlot() throws SQLException {
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

    @Override
    public void close() throws IOException {
        stop();
    }

    private boolean shouldProcessMessage(String msg) {
        return !msg.matches("\\{[\\s\\S]*\"change\":\\s*\\[\\s*\\]\\s*\\}$");
    }
}
