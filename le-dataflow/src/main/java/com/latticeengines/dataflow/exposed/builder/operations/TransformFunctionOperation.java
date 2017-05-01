package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.TransformFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class TransformFunctionOperation extends Operation {

    public TransformFunctionOperation(Input prior, String packageName, TransformDefinition definition) {

        FieldList fieldsToApply = new FieldList(definition.arguments.values().toArray(new String[] {}));
        FieldMetadata targetField = new FieldMetadata(definition.output, definition.type.type());

        RealTimeTransform transform = GetAndValidateRealTimeTransformUtils
                .fetchAndValidateRealTimeTransform(definition);

        TransformMetadata metadata = transform.getMetadata();

        TransformationMetadata overrideMetadata = definition.transformationMetadata;

        Map<String, String> properties = metadata.getProperties();

        if (overrideMetadata != null) {
            for (Map.Entry<String, String> entry : overrideMetadata.getProperties().entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        targetField.addProperties(properties);
        if (StringUtils.isNotEmpty(definition.outputDisplayName)) {
            targetField.setPropertyValue("DisplayName", definition.outputDisplayName);
        }
        // For now, assume that all Java functions are to be used within RTS
        setRTSProperties(targetField, definition.name, definition.arguments);
        Operation base = new FunctionOperation(prior, //
                new TransformFunction(definition, transform, DataFlowUtils.convertToFields(targetField.getFieldName())), //
                fieldsToApply, //
                targetField, null);

        this.metadata = base.getOutputMetadata();
        this.pipe = base.getOutputPipe();
    }

    private void setRTSProperties(FieldMetadata targetField, String moduleName, Map<String, Object> arguments) {
        // Add as an RTS function
        targetField.setPropertyValue("RTSAttribute", "true");
        targetField.setPropertyValue("RTSModuleName", moduleName);
        ObjectMapper mapper = new ObjectMapper();
        try {
            targetField.setPropertyValue("RTSArguments", mapper.writeValueAsString(arguments));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
