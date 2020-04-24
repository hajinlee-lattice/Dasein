package com.latticeengines.domain.exposed.datafabric;

public class GenericTableActivity extends BaseHdfsAvroFabricEntity<GenericTableActivity> implements FabricEntity<GenericTableActivity> {

    private String partitionKeyInHdfsRecord;
    private String sortKeyInHdfsRecord;

    public GenericTableActivity(String partitionKeyInHdfsRecord, String sortKeyInHdfsRecord) {
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
