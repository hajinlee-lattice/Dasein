package com.latticeengines.propdata.collection.service;

import com.latticeengines.propdata.collection.source.Source;

public interface CollectionDataFlowService {

    void executeMergeRawSnapshotData(Source source, String rawDir, String mergeDataFlowQualifier);

    void executePivotSnapshotData(Source source, String snapshotDir, String pivotDataFlowQualifier);

}
