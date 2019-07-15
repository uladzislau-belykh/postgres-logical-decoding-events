package com.panda.event.dto.json;

import com.google.gson.annotations.SerializedName;

import java.util.List;

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
