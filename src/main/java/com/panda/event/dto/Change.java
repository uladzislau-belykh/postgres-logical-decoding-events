package com.panda.event.dto;

import java.util.Objects;

public class Change<T> {
    private ChangeType type;
    private String table;
    private T oldValue;
    private T newValue;

    public Change() {
    }

    public Change(ChangeType type, String table, T oldValue, T newValue) {
        this.type = type;
        this.table = table;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Change<?> change = (Change<?>) o;
        return type == change.type &&
                Objects.equals(table, change.table) &&
                Objects.equals(oldValue, change.oldValue) &&
                Objects.equals(newValue, change.newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, table, oldValue, newValue);
    }
}
