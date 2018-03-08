package com.latticeengines.cdl.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlSplitParameters;

@Component("matchCdlSplitFlow")
public class MatchCdlSplitFlow extends TypesafeDataFlowBuilder<MatchCdlSplitParameters> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlSplitFlow.class);

    @Override
    public Node construct(MatchCdlSplitParameters parameters) {
        Node inputTable = addSource(parameters.inputTable);
        Node result = inputTable.filter(parameters.expression, new FieldList(parameters.filterField));
        return result;
    }

}
