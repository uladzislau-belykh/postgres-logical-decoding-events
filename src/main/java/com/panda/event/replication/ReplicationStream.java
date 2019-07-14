package com.panda.event.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
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
    private PGReplicationStream stream;
    private List<String> tables;

    public ReplicationStream(String slotName, ReplicationConnectionSource replicationConnectionSource, DataSource connectionSource, List<String> tables) {
        this.slotName = slotName;
        this.replicationConnectionSource = replicationConnectionSource;
        this.connectionSource = connectionSource;
        this.tables = tables;
    }

    public ReplicationEvent receive() {
        logger.debug("Receive message");
        try {
            ByteBuffer buffer;
            PGReplicationStream stream = getStream();
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

    public boolean commit(LogSequenceNumber lsn) {
        stream.setAppliedLSN(lsn);
        stream.setFlushedLSN(lsn);
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

    private PGReplicationStream getStream() throws SQLException {
        PGConnection connection = replicationConnectionSource.getConnection();
        if (stream == null || stream.isClosed()) {
            stream = createReplicationStream(connection);
        }
        return stream;
    }

    private PGReplicationStream createReplicationStream(PGConnection connection) throws SQLException {
        return connection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("include-xids", true)
                .withSlotOption("pretty-print", true)
                .withSlotOption("include-timestamp", true)
                .withSlotOption("include-types", false)
//                .withSlotOption("include-unchanged-toast", false)
                .withSlotOption("add-tables", String.join(", ", this.tables))
                .withStatusInterval(15, TimeUnit.SECONDS)
                .start();
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
            closeStream();
        } catch (SQLException e) {
        }
    }

    private void closeStream() throws SQLException {
        if (stream != null && !stream.isClosed()) {
            stream.close();
        }
    }
}
