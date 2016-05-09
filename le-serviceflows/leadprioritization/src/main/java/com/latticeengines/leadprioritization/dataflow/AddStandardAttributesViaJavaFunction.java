package com.latticeengines.leadprioritization.dataflow;

import java.util.Map;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

@Component("addStandardAttributesViaJavaFunction")
public class AddStandardAttributesViaJavaFunction extends TypesafeDataFlowBuilder<AddStandardAttributesParameters> {

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AddStandardAttributes.class);

    @Override
    public Node construct(AddStandardAttributesParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        Node last = eventTable;

        Attribute emailOrWebsite = eventTable.getSourceAttribute(InterfaceName.Email) != null //
        ? eventTable.getSourceAttribute(InterfaceName.Email) //
                : eventTable.getSourceAttribute(InterfaceName.Website);

        Map<String, TransformDefinition> definitions = TransformationPipeline.definitions;
        definitions.get(TransformationPipeline.stdLengthDomain.name + "_" //
                + TransformationPipeline.stdLengthDomain.output) //
        .arguments.put("column", emailOrWebsite.getName());

        for (Map.Entry<String, TransformDefinition> entry : definitions.entrySet()) {
            last = addFunction(last, entry.getValue());
        }
        return last;
    }

    private Node addFunction(Node last, TransformDefinition definition) {
        return last.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
    }
}
