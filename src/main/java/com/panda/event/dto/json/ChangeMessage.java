package com.panda.event.dto.json;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ChangeMessage {

    private String kind;

    private String table;

    @SerializedName("columnnames")
    private List<String> columnNames;

    @SerializedName("columnvalues")
    private List<String> columnValues;

    @SerializedName("oldkeys")
    private OldKeys oldKeys;

    public ChangeMessage() {
    }

    public ChangeMessage(String kind, String table, List<String> columnNames, List<String> columnValues, OldKeys oldKeys) {
        this.kind = kind;
        this.table = table;
        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.oldKeys = oldKeys;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<String> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(List<String> columnValues) {
        this.columnValues = columnValues;
    }

    public OldKeys getOldKeys() {
        return oldKeys;
    }

    public void setOldKeys(OldKeys oldKeys) {
        this.oldKeys = oldKeys;
    }

    @Override
    public String toString() {
        return "ChangeMessage{" +
                "kind='" + kind + '\'' +
                ", table='" + table + '\'' +
                ", columnNames=" + columnNames +
                ", columnValues=" + columnValues +
                ", oldKeys=" + oldKeys +
                '}';
    }

    public class OldKeys{
        @SerializedName("keynames")
        private List<String> keyNames;
        @SerializedName("keyvalues")
        private List<String> keyValues;

        public OldKeys() {
        }

        public OldKeys(List<String> keyNames, List<String> keyValues) {
            this.keyNames = keyNames;
            this.keyValues = keyValues;
        }

        public List<String> getKeyNames() {
            return keyNames;
        }

        public void setKeyNames(List<String> keyNames) {
            this.keyNames = keyNames;
        }

        public List<String> getKeyValues() {
            return keyValues;
        }

        public void setKeyValues(List<String> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public String toString() {
            return "OldKeysJson{" +
                    "keyNames=" + keyNames +
                    ", keyValues=" + keyValues +
                    '}';
        }
    }
}
