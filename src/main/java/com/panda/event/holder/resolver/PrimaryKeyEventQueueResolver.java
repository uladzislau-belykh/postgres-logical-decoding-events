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

package com.panda.event.holder.resolver;

import com.panda.event.dto.Change;
import com.panda.event.dto.ChangeType;
import com.panda.event.holder.provider.PrimaryKeyProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Uladzislau Belykh
 */
public class PrimaryKeyEventQueueResolver implements EventQueueResolver {

    private PrimaryKeyProvider primaryKeyResolver;

    public PrimaryKeyEventQueueResolver(PrimaryKeyProvider primaryKeyResolver) {
        Objects.requireNonNull(primaryKeyResolver);
        this.primaryKeyResolver = primaryKeyResolver;
    }

    @Override
    public int resolve(int queueCount, Change<Map<String, String>> event) {
        Map<String, String> values;
        if(event.getType().equals(ChangeType.DELETE)){
            values = event.getOldValue();
        }else{
            values = event.getNewValue();
        }
        List<String> primaryKeys = primaryKeyResolver.getPrimaryKeys(event.getTable());
        int hashCode = Arrays.hashCode(primaryKeys.stream().map(values::get).toArray());
        return hashCode % queueCount;
    }
}
