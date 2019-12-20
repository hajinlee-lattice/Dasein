package com.latticeengines.datacloud.dataflow.transformation.source;


import java.util.List;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.atlas.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

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

        //FIX-ME: assume the first group-by key is domain
        String domainField = groupByKeys.get(0);
        src = src.apply(new DomainCleanupFunction(domainField, true), new FieldList(domainField),
                new FieldMetadata(domainField, String.class));

        return src.groupByAndLimit(new FieldList(groupByKeys), new FieldList(sortByKey), //
                1, true, false);
    }
}
