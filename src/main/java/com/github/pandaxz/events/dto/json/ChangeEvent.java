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

package com.github.pandaxz.events.dto.json;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * @author Uladzislau Belykh
 */
public class ChangeEvent {

    @SerializedName("timestamp")
    private String commitTime;
    @SerializedName("change")
    private List<ChangeMessage> changes;
    @SerializedName("nextlsn")
    private String nextLsn;

    public ChangeEvent(String commitTime, List<ChangeMessage> changes, String nextLsn) {
        this.commitTime = commitTime;
        this.changes = changes;
        this.nextLsn = nextLsn;
    }

    public ChangeEvent() {
    }

    public String getNextLsn() {
        return nextLsn;
    }

    public void setNextLsn(String nextLsn) {
        this.nextLsn = nextLsn;
    }

    public List<ChangeMessage> getChanges() {
        return changes;
    }

    public void setChanges(List<ChangeMessage> changes) {
        this.changes = changes;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(String commitTime) {
        this.commitTime = commitTime;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
                "commitTime='" + commitTime + '\'' +
                ", changes=" + changes +
                ", nextLsn=" + nextLsn +
                '}';
    }
}
