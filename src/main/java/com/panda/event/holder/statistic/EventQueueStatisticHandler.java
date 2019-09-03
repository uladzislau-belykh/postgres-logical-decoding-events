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

package com.panda.event.holder.statistic;

import com.panda.event.dto.Change;

import java.time.Instant;
import java.util.Map;

public class EventQueueStatisticHandler {

    private String table;
    private int queueNumber;
    private EventHolderStatisticHandler statisticHandler;
    private String handlerName;

    public EventQueueStatisticHandler(String table, int queueNumber, EventHolderStatisticHandler statisticHandler, String handlerName) {
        this.table = table;
        this.queueNumber = queueNumber;
        this.statisticHandler = statisticHandler;
        this.handlerName = handlerName;
    }

    public void eventAddedToQueue(Instant timestamp, Change<Map<String, String>> event) {
        this.statisticHandler.eventAddedToQueue(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventPolledFromQueue(Instant timestamp, Change<Map<String, String>> event){
        this.statisticHandler.eventPolledFromQueue(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventHandled(Instant timestamp, Change<Map<String, String>> event) {
        this.statisticHandler.eventHandled(this.table, this.queueNumber, this.handlerName, timestamp, event);
    }

    public void eventHandled(String handlerName, Long duration, Change<Map<String, String>> event) {
        this.statisticHandler.eventHandled(this.table, this.queueNumber, handlerName, duration, event);
    }
}
