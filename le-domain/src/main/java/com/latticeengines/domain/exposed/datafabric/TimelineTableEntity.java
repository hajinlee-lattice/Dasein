package com.latticeengines.domain.exposed.datafabric;

public class TimelineTableEntity extends BaseHdfsAvroFabricEntity<TimelineTableEntity> implements FabricEntity<TimelineTableEntity> {

    private String partitionKeyInHdfsRecord;
    private String sortKeyInHdfsRecord;

    public TimelineTableEntity(String partitionKeyInHdfsRecord, String sortKeyInHdfsRecord) {
        this.partitionKeyInHdfsRecord = partitionKeyInHdfsRecord;
        this.sortKeyInHdfsRecord = sortKeyInHdfsRecord;
    }

    @Override
    public String getPartitionKeyInHdfsRecord() {
        return partitionKeyInHdfsRecord;
    }

    @Override
    public String getSortKeyInHdfsRecord() {
        return sortKeyInHdfsRecord;
    }
}
