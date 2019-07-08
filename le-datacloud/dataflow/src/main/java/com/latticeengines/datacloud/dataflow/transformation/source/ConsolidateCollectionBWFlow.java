package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;

@Component(ConsolidateCollectionBWFlow.BEAN_NAME)
public class ConsolidateCollectionBWFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionBWFlow";

    private Node renameTimestampField(Node node) {
        return node.rename(new FieldList(Arrays.asList("LE_Last_Upload_Date")),
                new FieldList(Arrays.asList("CollectedAt")));
    }

    @Override
    public Node construct(ConsolidateCollectionParameters parameters) {
        Node input = addSource(parameters.getBaseTables().get(0));

        //combine legacy bw consolidated result
        if (parameters.getBaseTables().size() == 2) {

            Node legacy = addSource(parameters.getBaseTables().get(1));
            legacy = renameTimestampField(legacy).retain(input.getFieldNamesArray());

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
        return src;
    }
}
