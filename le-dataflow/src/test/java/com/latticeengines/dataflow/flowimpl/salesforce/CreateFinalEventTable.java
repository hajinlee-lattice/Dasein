package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("createFinalEventTable")
public class CreateFinalEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        addSource("EventTable", sources.get("EventTable"));
        addSource("Opportunity", sources.get("Opportunity"));

        String eventTable$oppty = addInnerJoin("EventTable", //
                new FieldList("ConvertedOpportunityId"), //
                "Opportunity", //
                new FieldList("Id"));

        // Create the event
        FieldMetadata event = new FieldMetadata("P1_Event", Boolean.class);
        event.setPropertyValue("length", "0");
        event.setPropertyValue("precision", "0");
        event.setPropertyValue("scale", "0");
        String eventDefinition = dataFlowCtx.getProperty("EVENTDEFNEXPR", String.class);
        String[] eventDefinitionColsUsed = dataFlowCtx.getProperty("EVENTDEFNCOLS", String[].class);
        String withEvent = addFunction(eventTable$oppty, eventDefinition, new FieldList(eventDefinitionColsUsed), event);

        // Convert booleans to integers
        List<FieldMetadata> metadata = getMetadata(withEvent);

        String convertBooleanToInt = withEvent;
        for (FieldMetadata m : metadata) {
            if (m.getJavaType() == Boolean.class) {
                FieldMetadata intM = new FieldMetadata(Type.INT, Integer.class, m.getFieldName(), null);
                convertBooleanToInt = addFunction(convertBooleanToInt, m.getFieldName() + " == true ? 1 : 0",
                        new FieldList(m.getFieldName()), intM);
            }
        }
        return convertBooleanToInt;
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }
}
