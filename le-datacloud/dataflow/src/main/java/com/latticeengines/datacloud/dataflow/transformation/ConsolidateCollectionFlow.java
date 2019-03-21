package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.ConsolidateCollectionFlow.BEAN_NAME;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;

@Component(BEAN_NAME)
public abstract class ConsolidateCollectionFlow extends TypesafeDataFlowBuilder<ConsolidateCollectionParameters> {

    public static final String BEAN_NAME = "consolidateCollectionFlow";

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

    private Node findMostRecent(Node src, ConsolidateCollectionParameters parameters)
    {
        List<String> groupByKeys = parameters.getGroupBy();
        String sortByKey = parameters.getSortBy();
        return src.groupByAndLimit(new FieldList(groupByKeys), new FieldList(sortByKey), //
                1, true, false);
    }
}
