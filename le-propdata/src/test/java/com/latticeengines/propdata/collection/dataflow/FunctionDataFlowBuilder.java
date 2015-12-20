package com.latticeengines.propdata.collection.dataflow;

import java.util.Collections;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.dataflow.function.DomainCleanupFunction;

import cascading.operation.Function;


@Component("functionDataFlowBuilder")
public class FunctionDataFlowBuilder extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        String source = addSource("Source", sources.get("Source"),
                Collections.singletonList(new FieldMetadata("Domain", String.class)));

        Function function = new DomainCleanupFunction("Domain");
        return addFunction(source, function, new FieldList("Domain"), new FieldMetadata("Domain", String.class));
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");

    }

}
