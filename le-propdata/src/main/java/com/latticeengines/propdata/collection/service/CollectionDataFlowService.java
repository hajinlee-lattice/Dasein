package com.latticeengines.propdata.collection.service;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.pivot.PivotMapper;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

public interface CollectionDataFlowService {

    void executeMergeRawSnapshotData(Source source, String mergeDataFlowQualifier, String uid);

    void executePivotData(PivotedSource source, String snapshotDir, DataFlowBuilder.FieldList groupByFields,
                          PivotMapper pivotMapper, String uid);

    void executeJoin(String lhsPath, String rhsPath, String outputDir, String dataflowBean);

}
