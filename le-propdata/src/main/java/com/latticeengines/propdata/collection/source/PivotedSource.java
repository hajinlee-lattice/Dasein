package com.latticeengines.propdata.collection.source;

import java.util.HashMap;
import java.util.Map;

public enum PivotedSource implements Source {

    FEATURE_PIVOTED("FeaturePivoted", "Feature_Pivoted_Source", CollectionSource.FEATURE, "featurePivotService"),
    TEST_PIVOTED("TestPivoted", "TestPivoted", CollectionSource.TEST_COLLECTION, "testPivotService");

    private final String sourceName;
    private final String tableName;
    private final Source baseSource;
    private final String refreshBean;

    private static Map<String, PivotedSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (PivotedSource source: PivotedSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    PivotedSource(String sourceName, String tableName, Source baseSource, String refreshBean) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.baseSource = baseSource;
        this.refreshBean = refreshBean;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getTableName() { return tableName; }

    @Override
    public String getRefreshServiceBean() { return refreshBean; }

    public Source getBaseSource() { return baseSource; }

    public static PivotedSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
