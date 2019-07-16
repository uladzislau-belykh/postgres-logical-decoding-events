package com.panda.event.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicationStream implements Closeable {

    public static final String OUTPUT_PLUGIN = "wal2json";

    private static final Logger logger = LoggerFactory.getLogger(ReplicationStream.class);

    private String slotName;
    private ReplicationConnectionSource replicationConnectionSource;
    private DataSource connectionSource;
    private ReplicationStreamSource streamHolder;

    public ReplicationStream(String slotName, ReplicationConnectionSource replicationConnectionSource, DataSource connectionSource, List<String> tables) {
        this.slotName = slotName;
        this.replicationConnectionSource = replicationConnectionSource;
        this.connectionSource = connectionSource;
        this.streamHolder = new ReplicationStreamSource(slotName, replicationConnectionSource, tables);
    }

    public ReplicationEvent receive() {
        logger.debug("Receive message");
        try {
            ByteBuffer buffer;
            PGReplicationStream stream = streamHolder.getStream();
            buffer = stream.readPending();
            Instant readTime = Instant.now();
            LogSequenceNumber lastReceiveLSN = stream.getLastReceiveLSN();
            if (buffer == null) {
                return null;
            }

            int offset = buffer.arrayOffset();
            byte[] source = buffer.array();
            int length = source.length - offset;
            String msg = new String(source, offset, length);
            return new ReplicationEvent(msg, readTime, lastReceiveLSN);
        } catch (Exception e) {
            logger.info("Error when receiving: ", e);
            replicationConnectionSource.invalidateConnection();
            return null;
        }
    }

    public boolean commit(LogSequenceNumber lastReceiveLSN, LogSequenceNumber nextLsn) {
        if (nextLsn != null) {
            streamHolder.commitNextLsn(nextLsn);
        }
        PGReplicationStream stream;
        try {
            stream = streamHolder.getStream();
        } catch (SQLException e) {
            return false;
        }
        stream.setAppliedLSN(lastReceiveLSN);
        stream.setFlushedLSN(lastReceiveLSN);
        return silentlyUpdateStatus(stream);
    }

    public void resetUncomitted() {
        replicationConnectionSource.invalidateConnection();
    }

    public boolean dropSlot() throws SQLException {
        if (!slotExists()) {
            return true;
        }
        PGConnection connection = replicationConnectionSource.getConnection();
        try {
            connection.getReplicationAPI().dropReplicationSlot(slotName);
        } catch (SQLException e) {
            replicationConnectionSource.invalidateConnection();
            throw e;
        }
        logger.debug(slotName+ " - slot is dropped");
        return true;

    }

    public boolean createSlot() throws SQLException {
        if (slotExists()) {
            return true;
        }
        PGConnection connection = replicationConnectionSource.getConnection();
        try {
            connection.getReplicationAPI().createReplicationSlot().logical()
                    .withSlotName(slotName)
                    .withOutputPlugin(OUTPUT_PLUGIN)
                    .make();
        } catch (SQLException e) {
            replicationConnectionSource.invalidateConnection();
            throw e;
        }
        logger.debug(slotName+ " - slot is created");
        return true;
    }

    public boolean slotExists() throws SQLException {
        try (Connection connection = connectionSource.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT count(*)>0 FROM pg_replication_slots where slot_name = '" + slotName + "'");
            rs.next();
            return rs.getBoolean(1);
        }
    }

    private boolean silentlyUpdateStatus(PGReplicationStream stream) {
        if (stream != null) {
            try {
                stream.forceUpdateStatus();
                return true;
            } catch (Exception e) {
                replicationConnectionSource.invalidateConnection();
                logger.error("error to update status", e);
            }
        }
        return false;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public void setReplicationConnectionSource(ReplicationConnectionSource replicationConnectionSource) {
        this.replicationConnectionSource = replicationConnectionSource;
    }

    public void setConnectionSource(DataSource connectionSource) {
        this.connectionSource = connectionSource;
    }

    public String getSlotName() {
        return slotName;
    }

    public ReplicationConnectionSource getReplicationConnectionSource() {
        return replicationConnectionSource;
    }

    public DataSource getConnectionSource() {
        return connectionSource;
    }

    @Override
    public void close() throws IOException {
        try {
            streamHolder.closeStream();
        } catch (SQLException e) {
        }
    }

    private class ReplicationStreamSource {
        private PGReplicationStream stream;
        private volatile boolean reconnectRequired = true;
        private List<String> tables;
        private ReplicationConnectionSource replicationConnectionSource;
        private String slotName;
        private LogSequenceNumber nextLsn;

        public ReplicationStreamSource(String slotName, ReplicationConnectionSource replicationConnectionSource, List<String> tables) {
            this.tables = tables;
            this.replicationConnectionSource = replicationConnectionSource;
            this.replicationConnectionSource.registerSubscriber(() -> this.reconnectRequired = true);
            this.slotName = slotName;
        }

        public PGReplicationStream getStream() throws SQLException {
            PGConnection connection = this.replicationConnectionSource.getConnection();
            if (this.stream == null || this.stream.isClosed() || this.reconnectRequired) {
                this.stream = createReplicationStream(connection);
                this.reconnectRequired = false;
            }
            return this.stream;
        }

        private PGReplicationStream createReplicationStream(PGConnection connection) throws SQLException {
            ChainedLogicalStreamBuilder streamBuilder = connection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(this.slotName);
            if (nextLsn != null && !nextLsn.equals(LogSequenceNumber.INVALID_LSN)) {
                streamBuilder.withStartPosition(nextLsn);
            }
            return streamBuilder
                    .withSlotOption("include-xids", true)
                    .withSlotOption("pretty-print", true)
                    .withSlotOption("include-timestamp", true)
                    .withSlotOption("include-types", false)
                    .withSlotOption("include-lsn", true)
//                .withSlotOption("include-unchanged-toast", false)
                    .withSlotOption("add-tables", String.join(", ", this.tables))
                    .withStatusInterval(15, TimeUnit.SECONDS)
                    .start();
        }

        public void closeStream() throws SQLException {
            if (stream != null && !stream.isClosed()) {
                stream.close();
                reconnectRequired = true;
            }
        }

        public void commitNextLsn(LogSequenceNumber lsn) {
            this.nextLsn = lsn;
        }
    }
}
