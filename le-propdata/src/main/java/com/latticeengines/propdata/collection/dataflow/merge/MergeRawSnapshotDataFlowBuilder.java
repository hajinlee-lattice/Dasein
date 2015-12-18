package com.latticeengines.propdata.collection.dataflow.merge;

import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

import cascading.tuple.Fields;

public abstract class MergeRawSnapshotDataFlowBuilder extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        for (Map.Entry<String, String> source: sources.entrySet()) {
            addSource(source.getKey(), source.getValue());
        }

        String[] sourceNames = sources.keySet().toArray(new String[sources.size()]);

        return addGroupByAndFirst(sourceNames, uniqueFields(), sortFields());
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }

    protected abstract Fields uniqueFields();

    protected abstract Fields sortFields();

}
