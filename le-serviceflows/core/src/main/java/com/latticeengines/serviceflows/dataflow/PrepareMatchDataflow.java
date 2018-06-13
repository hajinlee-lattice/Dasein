package com.latticeengines.serviceflows.dataflow;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.PrepareMatchDataParameters;

@Component("prepareMatchDataflow")
public class PrepareMatchDataflow extends TypesafeDataFlowBuilder<PrepareMatchDataParameters> {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchDataflow.class);

    @Override
    public Node construct(PrepareMatchDataParameters parameters) {
        Node source = addSource(parameters.sourceTableName);
        Node result = source.retain(new FieldList(parameters.matchFields));
        if (StringUtils.isNotEmpty(parameters.matchGroupId)) {
            result = result.groupByAndLimit(new FieldList(parameters.matchGroupId), 1);
        }
        return result;
    }

}
