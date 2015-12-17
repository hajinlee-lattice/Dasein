package com.latticeengines.propdata.collection.source;

import java.util.HashMap;
import java.util.Map;

public enum PivotedSource implements Source {

    FEATURE_PIVOTED("FeaturePivoted", "Feature_Pivoted_Source", CollectionSource.FEATURE);

    private final String sourceName;
    private final String tableName;
    private final Source baseSource;

    private static Map<String, PivotedSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (PivotedSource source: PivotedSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    PivotedSource(String sourceName, String tableName, Source baseSource) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.baseSource = baseSource;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getTableName() { return tableName; }

    public Source getBaseSource() { return baseSource; }

    public static PivotedSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
