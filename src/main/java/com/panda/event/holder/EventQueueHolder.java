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

package com.panda.event.holder;

import com.panda.event.dto.Change;
import com.panda.event.holder.statistic.EventHolderStatisticHandler;

import java.io.Closeable;
import java.util.Map;

/**
* @author Uladzislau Belykh
*/
public interface EventQueueHolder extends Closeable {
    void init(EventHolderStatisticHandler statisticHandler);

    void add(Change<Map<String, String>> event);

    void registerHandler(EventHandler handler);

    void unregisterHandler(EventHandler handler);
}
