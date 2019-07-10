package com.panda.event.replication.dto.json;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ChangeEvent {

    @SerializedName("timestamp")
    private String commitTime;
    @SerializedName("change")
    private List<ChangeMessage> changes;

    public ChangeEvent(String commitTime, List<ChangeMessage> changes) {
        this.commitTime = commitTime;
        this.changes = changes;
    }

    public ChangeEvent() {
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
                "commitTime=" + commitTime +
                ", changes=" + changes +
                '}';
    }
}
