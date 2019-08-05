package com.panda.event.holder.resolver;

import com.panda.event.dto.Change;
import com.panda.event.dto.ChangeType;
import com.panda.event.holder.provider.PrimaryKeyProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
