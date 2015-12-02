package com.latticeengines.propdata.collection.service;

public interface CollectionDataFlowService {

    void executeMergeRawSnapshotData(String sourceName, String rawDir, String mergeDataFlowQualifier);

    void executePivotSnapshotData(String sourceName, String snapshotDir, String pivotDataFlowQualifier);

}
