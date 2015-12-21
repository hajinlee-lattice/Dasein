package com.latticeengines.propdata.collection.source;

import java.util.HashMap;
import java.util.Map;

public enum CollectionSource implements Source {

    BUILT_WITH("BuiltWith", "BuiltWith_MostRecent", "builtWithArchiveService"),
    FEATURE("Feature", "Feature_MostRecent", "featureArchiveService"),
    TEST_COLLECTION("TestCollection", "TestCollection", "testArchiveService");

    private final String sourceName;
    private final String tableName;
    private final String serviceBean;

    private static Map<String, CollectionSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (CollectionSource source: CollectionSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    CollectionSource(String sourceName, String tableName, String serviceBean) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.serviceBean = serviceBean;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getTableName() { return tableName; }

    @Override
    public String getRefreshServiceBean() { return serviceBean; }

    public static CollectionSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
