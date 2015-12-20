package com.latticeengines.propdata.collection.source;

import java.util.HashMap;
import java.util.Map;

public enum CollectionSource implements Source {

    BUILTWITH("BuiltWith", "BuiltWith_MostRecent"),
    FEATURE("Feature", "Feature_MostRecent");

    private final String sourceName;
    private final String tableName;

    private static Map<String, CollectionSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (CollectionSource source: CollectionSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    CollectionSource(String sourceName, String tableName) {
        this.sourceName = sourceName;
        this.tableName = tableName;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getTableName() { return tableName; }

    public static CollectionSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
