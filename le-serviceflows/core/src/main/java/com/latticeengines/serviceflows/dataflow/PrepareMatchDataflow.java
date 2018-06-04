package com.latticeengines.serviceflows.dataflow;

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
        source = source.retain(new FieldList(parameters.matchFields));
        return source;
    }

}
