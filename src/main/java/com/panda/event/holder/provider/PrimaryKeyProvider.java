package com.panda.event.holder.provider;

import java.util.List;

public interface PrimaryKeyProvider {
    List<String> getPrimaryKeys(String table);
}
