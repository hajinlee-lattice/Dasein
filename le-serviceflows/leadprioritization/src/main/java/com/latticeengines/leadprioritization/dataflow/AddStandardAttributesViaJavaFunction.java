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
            last = addFunction(last, eventTable, entry.getValue());
        }
        return last;
    }

    private Node addFunction(Node last, Node eventTable, TransformDefinition definition) {
        for (Object value : definition.arguments.values()) {
            Attribute attr = eventTable.getSourceAttribute(String.valueOf(value));
            if (attr == null) {
                log.info(String.format(
                        "Excluding field %s (function %s) because some source columns are not available",
                        definition.output, definition.name));
                return last;
            }
        }
        return last.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
    }
}
