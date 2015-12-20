package com.latticeengines.propdata.collection.dataflow;

import java.util.Arrays;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;


@Component("innerJoinDataFlowBuilder")
public class InnerJoinDataFlowBuilder extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        String source1 = addSource("Source1", sources.get("Source1"), Arrays.asList(
                new FieldMetadata("ID", Integer.class),
                new FieldMetadata("Domain", String.class)
        ));
        String source2 = addSource("Source2", sources.get("Source2"));
        return addJoin(source1, new FieldList("Domain"), source2, new FieldList("URL"), JoinType.INNER);
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");

    }

}
