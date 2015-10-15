package com.latticeengines.propdata.collection.dataflow.merge;

import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;

import cascading.tuple.Fields;

public abstract class MergeRawSnapshotDataFlowBuilder extends CascadingDataFlowBuilder {

    protected static final String RAW_SOURCE = "RawSource";
    protected static final String SNAPSHOT_SOURCE = "SnapshotSource";

    MergeRawSnapshotDataFlowBuilder() { super(false, false); }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        addSource(RAW_SOURCE, sources.get(CollectionDataFlowKeys.RAW_AVRO_SOURCE));
        addSource(SNAPSHOT_SOURCE, sources.get(CollectionDataFlowKeys.DEST_SNAPSHOT_SOURCE));

        return addGroupByAndFirst(new String[]{RAW_SOURCE, SNAPSHOT_SOURCE}, uniqueFields(), sortFields());
    }

    protected abstract Fields uniqueFields();

    protected abstract Fields sortFields();

}
