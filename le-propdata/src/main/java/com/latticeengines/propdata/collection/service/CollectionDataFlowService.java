package com.latticeengines.propdata.collection.service;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;

public interface CollectionDataFlowService {

    void executeMergeRawSnapshotData(CollectionSource source, String uid);

    void executePivotData(PivotedSource source, String baseVersion, DataFlowBuilder.FieldList groupByFields,
                          PivotStrategyImpl pivotStrategy, String uid);

    void executeJoin(String lhsPath, String rhsPath, String outputDir, String dataflowBean);

}
