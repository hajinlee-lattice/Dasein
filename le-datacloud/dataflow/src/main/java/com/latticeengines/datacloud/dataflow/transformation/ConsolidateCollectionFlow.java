package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.ConsolidateCollectionFlow.BEAN_NAME;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;

import cascading.operation.buffer.FirstNBuffer;

@Component(BEAN_NAME)
public class ConsolidateCollectionFlow extends TypesafeDataFlowBuilder<ConsolidateCollectionParameters> {

    public static final String BEAN_NAME = "consolidateCollectionFlow";

    @Override
    public Node construct(ConsolidateCollectionParameters parameters) {
        // assume the first base source is always the input
        Node input = addSource(parameters.getBaseTables().get(0));
        List<String> groupByKeys = parameters.getGroupBy();
        String sortByKey = parameters.getSortBy();
        Node mostRecent = input.groupByAndBuffer(new FieldList(groupByKeys), new FieldList(sortByKey), //
                new FirstNBuffer(1), true);
        return mostRecent;
    }

}
