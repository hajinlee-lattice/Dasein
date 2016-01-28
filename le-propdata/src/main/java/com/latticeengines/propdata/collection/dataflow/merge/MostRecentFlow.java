package com.latticeengines.propdata.collection.dataflow.merge;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.propdata.collection.dataflow.function.OldDataCleanupFunction;
import com.latticeengines.propdata.core.dataflow.function.DomainCleanupFunction;

@Component("mostRecentFlow")
public class MostRecentFlow extends TypesafeDataFlowBuilder<MostRecentDataFlowParameters>  {

    @Override
    public Node construct(MostRecentDataFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        return findMostRecent(source, parameters);
    }

    protected Node findMostRecent(Node source, MostRecentDataFlowParameters parameters){
        String[] groupbyFields = parameters.getGroupbyFields();
        String timestampField = parameters.getTimestampField();
        String domainField = parameters.getDomainField();
        source = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                new FieldMetadata(domainField, String.class));
        source = source.apply(new OldDataCleanupFunction(timestampField, parameters.getEarliest()),
                new FieldList(timestampField), new FieldMetadata(timestampField, Long.class));
        return source.groupByAndLimit(new FieldList(groupbyFields), new FieldList(timestampField), 1, true, true);
    }

}
