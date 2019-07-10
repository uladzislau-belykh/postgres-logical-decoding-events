package com.panda.event.replication.dto;

public class Change<T> {
    private ChangeType type;
    private String table;
    private T oldValue;
    private T newValue;

    public ChangeType getType() {
        return type;
    }

    public void setType(ChangeType type) {
        this.type = type;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public T getOldValue() {
        return oldValue;
    }

    public void setOldValue(T oldValue) {
        this.oldValue = oldValue;
    }

    public T getNewValue() {
        return newValue;
    }

    public void setNewValus(T newValue) {
        this.newValue = newValue;
    }

    @Override
    public String toString() {
        return "ChangeMessage{" +
                "type=" + type +
                ", table='" + table + '\'' +
                ", oldValue=" + oldValue +
                ", newValue=" + newValue +
                '}';
    }
}
