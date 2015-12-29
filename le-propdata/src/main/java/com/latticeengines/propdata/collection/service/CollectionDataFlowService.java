package com.latticeengines.propdata.collection.service;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.PivotedSource;

public interface CollectionDataFlowService {

    void executeMergeRawSnapshotData(CollectedSource source, String uid) ;

    void executePivotData(PivotedSource source, String baseVersion, DataFlowBuilder.FieldList groupByFields,
                          PivotStrategyImpl pivotStrategy, String uid);

}
