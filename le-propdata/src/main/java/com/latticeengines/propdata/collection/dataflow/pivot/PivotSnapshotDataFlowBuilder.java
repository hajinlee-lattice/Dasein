package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.runtime.cascading.PivotBuffer;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;

public abstract class PivotSnapshotDataFlowBuilder extends CascadingDataFlowBuilder {

    protected static final String SNAPSHOT_SOURCE = "SnapshotSource";

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        addSource(SNAPSHOT_SOURCE, sources.get(CollectionDataFlowKeys.SNAPSHOT_SOURCE));
        return addGroupByAndBuffer(SNAPSHOT_SOURCE, getGroupbyFields(), getPivotBuffer(), getFieldMetadatas());
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }

    abstract protected List<FieldMetadata> getFieldMetadatas();

    abstract protected FieldList getGroupbyFields();

    abstract protected PivotBuffer getPivotBuffer();

}
