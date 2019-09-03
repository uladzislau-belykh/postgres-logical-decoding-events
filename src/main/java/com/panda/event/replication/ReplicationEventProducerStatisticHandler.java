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

import com.panda.event.dto.json.ChangeEvent;

import java.time.Instant;

/**
 * Interface show current producing state.
 *
 * @author Uladzislau Belykh
 */
public interface ReplicationEventProducerStatisticHandler {

    /**
     * Producer is running.
     */
    void producerIsRunning();

    /**
     * Producer is stopped.
     */
    void producerIsStopped();

    /**
     * Event is received.
     */
    void eventIsReceived();

    /**
     * Event is handled.
     *
     * @param changes  the changes
     * @param readTime the read time
     */
    void eventIsHandled(ChangeEvent changes, Instant readTime);
}
