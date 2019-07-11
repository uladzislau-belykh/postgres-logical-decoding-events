package com.panda.event.holder;

import java.util.List;

public interface PrimaryKeyProvider {

    List<String> getPrimaryKeyFiels(String table);
}
