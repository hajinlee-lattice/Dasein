package com.latticeengines.domain.exposed.datafabric;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

public class GenericTableEntity extends BaseHdfsAvroFabricEntity<GenericTableEntity>
        implements FabricEntity<GenericTableEntity> {

    public static final String DUMMY_SORT_KEY = "0";

    private String keyPrefix;
    private String partitionKeyInHdfsRecord;
    private String sortKeyInHdfsRecord;

    public GenericTableEntity() {
        keyPrefix = "";
        partitionKeyInHdfsRecord = "";
        sortKeyInHdfsRecord = "";
    }

    public GenericTableEntity(String keyPrefix, String partitionKey, String sortKey) {
        this.keyPrefix = keyPrefix;
        this.partitionKeyInHdfsRecord = partitionKey;
        this.sortKeyInHdfsRecord = sortKey;
    }

    public String getPartitionKeyInHdfsRecord() {
        return partitionKeyInHdfsRecord;
    }

    public String getSortKeyInHdfsRecord() {
        return sortKeyInHdfsRecord;
    }

    protected void setFabricyPartitionKey(GenericRecord record) {
        String pkField = getPartitionKeyInHdfsRecord();
        if (StringUtils.isBlank(pkField)) {
            throw new IllegalArgumentException(
                    "Must specify primary key field in hdfs avro record.");
        }
        Object pkObj = record.get(pkField);
        setPartitionKey(toFabricPrimaryKey(pkObj));
    }

    private String toFabricPrimaryKey(Object pkObj) {
        String primaryKey;
        if (pkObj instanceof Utf8 || pkObj instanceof String) {
            primaryKey = pkObj.toString();
        } else {
            primaryKey = String.valueOf(pkObj);
        }
        return keyPrefix + "_" + primaryKey;
    }

    protected void setFabricySortKey(GenericRecord record) {
        String skField = getSortKeyInHdfsRecord();
        if (StringUtils.isNotBlank(skField)) {
            Object skObj = record.get(skField);
            if (skObj instanceof Utf8 || skObj instanceof String) {
                setSortKey(skObj.toString());
            } else {
                setSortKey(String.valueOf(skObj));
            }
        } else {
            setSortKey(DUMMY_SORT_KEY);
        }
    }

}
