package com.latticeengines.propdata.collection.service.impl;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;

public abstract class AbstractPivotService extends AbstractRefreshService implements PivotService {

    abstract DataFlowBuilder.FieldList getGroupByFields();

    abstract PivotStrategyImpl getPivotStrategy();

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executePivotData(
                (PivotedSource) getSource(),
                progress.getBaseSourceVersion(),
                getGroupByFields(),
                getPivotStrategy(),
                progress.getRootOperationUID()
        );
    }

    @Override
    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.PIVOT_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

}
