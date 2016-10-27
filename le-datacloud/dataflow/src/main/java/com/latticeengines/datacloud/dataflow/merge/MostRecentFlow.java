package com.latticeengines.datacloud.dataflow.merge;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.OldDataCleanupFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.MostRecentDataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("mostRecentFlow")
public class MostRecentFlow extends TypesafeDataFlowBuilder<MostRecentDataFlowParameters> {

    @Override
    public Node construct(MostRecentDataFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        return findMostRecent(source, parameters);
    }

    protected Node findMostRecent(Node source, MostRecentDataFlowParameters parameters) {
        String[] groupbyFields = parameters.getGroupbyFields();
        String timestampField = parameters.getTimestampField();
        String domainField = parameters.getDomainField();
        if (StringUtils.isNotEmpty(domainField)) {
            source = source.apply(new DomainCleanupFunction(domainField), new FieldList(domainField),
                    new FieldMetadata(domainField, String.class));
        }
        source = source.apply(new OldDataCleanupFunction(timestampField, parameters.getEarliest()), new FieldList(
                timestampField), new FieldMetadata(timestampField, Long.class));
        return source.groupByAndLimit(new FieldList(groupbyFields), new FieldList(timestampField), 1, true, true);
    }

}
