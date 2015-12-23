package com.latticeengines.propdata.collection.source.impl;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.propdata.collection.source.Source;

public enum PivotedSource implements Source {

    FEATURE_PIVOTED("FeaturePivoted",
            "Feature_Pivoted_Source",
            CollectionSource.FEATURE,
            "featurePivotService",
            new String[]{ "URL" },
            "Timestamp"
    ),

    TEST_PIVOTED("TestPivoted",
            "TestPivoted",
            CollectionSource.TEST_COLLECTION,
            "testPivotService",
            new String[]{ "URL" },
            ""
    );

    private final String sourceName;
    private final String tableName;
    private final Source baseSource;
    private final String refreshBean;
    private final String[] primaryKey;
    private final String timestampField;

    private static Map<String, PivotedSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (PivotedSource source: PivotedSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    PivotedSource(String sourceName, String tableName, Source baseSource, String refreshBean, String[] primaryKey,
                  String timestampField) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.baseSource = baseSource;
        this.refreshBean = refreshBean;
        this.primaryKey = primaryKey;
        this.timestampField = timestampField;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getSqlTableName() { return tableName; }

    @Override
    public String getRefreshServiceBean() { return refreshBean; }

    @Override
    public String[] getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String getTimestampField() { return timestampField; }

    public Source getBaseSource() { return baseSource; }

    public static PivotedSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
