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

package com.panda.event.replication;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Wrapper for {@link PGConnection}. It manages connection and reconnect, if necessary.
 *
 * @author Uladzislau Belykh
 */
public class ReplicationConnectionSource implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationEventProducer.class);

    private String jdbcUrl;
    private String username;
    private String password;
    private Connection connection;
    private boolean reconnectRequired = true;
    private Set<ReconnectSubscriber> reconnectSubscribers = new HashSet<>();

    /**
     * Instantiates a new Replication connection source.
     *
     * @param jdbcUrl  the jdbc url
     * @param username the username
     * @param password the password
     */
    public ReplicationConnectionSource(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * Create new connection, if not exists and return.
     *
     * @param reconnect if is true, close current connection(if exists) and create new one
     * @return the connection
     * @throws SQLException the sql exception
     */
//todo rewrite, use proxy to catch exceptions and invalidate connection
    public PGConnection getConnection(boolean reconnect) throws SQLException {
        if (reconnect || reconnectRequired) {
            closeConnection();
            reconnectRequired = true;
        }
        if (reconnectRequired || (connection != null && connection.isClosed()) || connection == null) {
            connection = createReplicationConnection();
            sendEvent();
            reconnectRequired = false;
        }
        return connection.unwrap(PGConnection.class);
    }

    /**
     Create new connection, if not exists and return.
     *
     * @return the connection
     * @throws SQLException the sql exception
     */
    public PGConnection getConnection() throws SQLException {
        return getConnection(false);
    }

    /**
     * Invalidate current connection.
     */
    public void invalidateConnection() {
        reconnectRequired = true;
    }

    /**
     * Register reconnect subscriber.
     *
     * @param callback the callback
     */
    public void registerSubscriber(ReconnectSubscriber callback){
        reconnectSubscribers.add(callback);
    }

    /**
     * Unregister reconnect subscriber.
     *
     * @param callback the callback
     */
    public void unregisterSubscriber(ReconnectSubscriber callback){
        reconnectSubscribers.remove(callback);
    }

    private Connection createReplicationConnection() throws SQLException {
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    private void sendEvent() {
        reconnectSubscribers.forEach(ReconnectSubscriber::reconnect);
    }

    private void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            closeConnection();
        } catch (SQLException e) {
        }
    }
}
