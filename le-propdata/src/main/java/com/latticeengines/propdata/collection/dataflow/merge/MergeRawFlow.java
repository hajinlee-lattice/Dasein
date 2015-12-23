package com.latticeengines.propdata.collection.dataflow.merge;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.propdata.collection.dataflow.function.DomainCleanupFunction;

@Component("mergeRawFlow")
public class MergeRawFlow extends TypesafeDataFlowBuilder<MergeDataFlowParameters>  {

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    public Node construct(MergeDataFlowParameters parameters) {
        String[] sources = parameters.getSourceTables();
        String timestampField = parameters.getTimestampField();
        String domainField = parameters.getDomainField();
        String[] uniqueFields = parameters.getPrimaryKeys();
        Node[] nodes = new Node[sources.length];
        for (int i = 0; i< sources.length; i++) {
            Node source = addSource(sources[i]);
            nodes[i] = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                    new FieldMetadata(domainField, String.class));
        }
        Node merged = mergeNodes(nodes);
        return merged.groupByAndLimit(new FieldList(uniqueFields), new FieldList(timestampField), 1, true, true);
    }

}
