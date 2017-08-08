package org.elasticsearch.index;

import org.elasticsearch.index.shard.IndexEventListener;

import java.util.List;

import static java.util.Collections.emptyList;

public interface IndexPlugin {

    default List<IndexEventListener> getListeners() {
        return emptyList();
    }
}
