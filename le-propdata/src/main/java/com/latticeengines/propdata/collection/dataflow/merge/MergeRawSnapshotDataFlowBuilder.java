package com.latticeengines.propdata.collection.dataflow.merge;

import java.util.Map;

import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;

public abstract class MergeRawSnapshotDataFlowBuilder extends CascadingDataFlowBuilder {

    protected static final String RAW_SOURCE = "RawSource";
    protected static final String SNAPSHOT_SOURCE = "SnapshotSource";

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        addSource(RAW_SOURCE, sources.get(CollectionDataFlowKeys.RAW_AVRO_SOURCE));
        addSource(SNAPSHOT_SOURCE, sources.get(CollectionDataFlowKeys.DEST_SNAPSHOT_SOURCE));

        return addGroupByAndFirst(new String[] { RAW_SOURCE, SNAPSHOT_SOURCE }, uniqueFields(), sortFields());
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }

    protected abstract Fields uniqueFields();

    protected abstract Fields sortFields();

}
