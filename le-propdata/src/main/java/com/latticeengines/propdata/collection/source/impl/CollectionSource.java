package com.latticeengines.propdata.collection.source.impl;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.Source;

public enum CollectionSource implements Source, DomainBased {

    BUILT_WITH("BuiltWith",
            "BuiltWith_MostRecent",
            "BuiltWith",
            "builtWithArchiveService",
            "Domain",
            "LE_Last_Upload_Date",
            new String[]{"Domain", "Technology_Name"}
    ),

    FEATURE("Feature",
            "Feature_MostRecent",
            "Feature",
            "featureArchiveService",
            "URL",
            "LE_Last_Upload_Date",
            new String[]{"URL", "Feature"}
    ),

    TEST_COLLECTION("TestCollection",
            "TestCollection",
            "",
            "testArchiveService",
            "URL",
            "LE_Last_Upload_Date",
            new String[]{}
    );


    private final String sourceName;
    private final String tableName; // the table served to users
    private final String collectedTable; // the table collectors write to
    private final String serviceBean;
    private final String domainField;
    private final String timestampField;
    private final String[] primaryKey;

    private static Map<String, CollectionSource> sourceNameMap;

    static {
        sourceNameMap = new HashMap<>();
        for (CollectionSource source: CollectionSource.values()) {
            sourceNameMap.put(source.sourceName, source);
        }
    }

    CollectionSource(String sourceName, String tableName, String collectedTable, String serviceBean,
                     String domainField, String timestampField, String[] primaryKey) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.collectedTable = collectedTable;
        this.serviceBean = serviceBean;
        this.domainField = domainField;
        this.timestampField = timestampField;
        this.primaryKey = primaryKey;
    }

    @Override
    public String getSourceName() { return sourceName; }

    @Override
    public String getSqlTableName() { return tableName; }

    public String getCollectedTable() {
        return collectedTable;
    }

    @Override
    public String getRefreshServiceBean() { return serviceBean; }

    @Override
    public String[] getPrimaryKey() { return primaryKey; }

    @Override
    public String getTimestampField() { return timestampField; }

    @Override
    public String getDomainField() {
        return domainField;
    }

    public static CollectionSource fromSourceName(String sourceName) {
        return sourceNameMap.get(sourceName);
    }
}
