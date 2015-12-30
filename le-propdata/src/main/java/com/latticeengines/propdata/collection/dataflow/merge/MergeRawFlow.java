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
        Node source = addSource(parameters.getSourceTable());
        String[] groupbyFields = parameters.getGroupbyFields();
        String timestampField = parameters.getTimestampField();
        String domainField = parameters.getDomainField();
        source = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                new FieldMetadata(domainField, String.class));
        return source.groupByAndLimit(new FieldList(groupbyFields), new FieldList(timestampField), 1, true, true);
    }

}
