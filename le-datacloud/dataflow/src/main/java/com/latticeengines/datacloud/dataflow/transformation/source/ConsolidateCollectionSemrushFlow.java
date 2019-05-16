package com.latticeengines.datacloud.dataflow.transformation.source;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(ConsolidateCollectionSemrushFlow.BEAN_NAME)
public class ConsolidateCollectionSemrushFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionSemrushFlow";
    private static final String FIELD_RANK = "Rank";

    @Override
    public Node construct(ConsolidateCollectionParameters parameters) {
        Node input = addSource(parameters.getBaseTables().get(0));

        //combine legacy bw consolidated result
        if (parameters.getBaseTables().size() == 2) {

            Node legacy = addSource(parameters.getBaseTables().get(1)).retain(input.getFieldNamesArray());

            input = input.merge(legacy);

        }

        Node src = preRecentTransform(input, parameters);

        Node recent = findMostRecent(src, parameters);

        return postRecentTransform(recent, parameters);
    }

    @Override
    protected Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return src;
    }

    @Override
    protected Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        src = src.apply(
                String.format("%s == 0 ? null : %s", FIELD_RANK, FIELD_RANK), new FieldList(
                        FIELD_RANK), new FieldMetadata(FIELD_RANK, Integer.class));
        return src;
    }
}
