package com.latticeengines.datacloud.dataflow.transformation.source;


import java.util.List;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;

public abstract class ConsolidateCollectionFlow extends TypesafeDataFlowBuilder<ConsolidateCollectionParameters> {

    @Override
    public Node construct(ConsolidateCollectionParameters parameters) {
        // assume the first base source is always the input
        Node input = addSource(parameters.getBaseTables().get(0));

        Node src = preRecentTransform(input, parameters);

        Node recent = findMostRecent(src, parameters);

        return postRecentTransform(recent, parameters);
    }

    protected abstract Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters);

    protected abstract Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters);

    protected Node findMostRecent(Node src, ConsolidateCollectionParameters parameters)
    {
        List<String> groupByKeys = parameters.getGroupBy();
        String sortByKey = parameters.getSortBy();
        return src.groupByAndLimit(new FieldList(groupByKeys), new FieldList(sortByKey), //
                1, true, false);
    }
}
