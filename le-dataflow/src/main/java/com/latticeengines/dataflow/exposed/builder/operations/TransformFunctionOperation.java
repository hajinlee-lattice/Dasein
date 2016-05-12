package com.latticeengines.dataflow.exposed.builder.operations;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.TransformFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class TransformFunctionOperation extends Operation {

    private static final Log log = LogFactory.getLog(TransformFunctionOperation.class);

    @SuppressWarnings("unchecked")
    public TransformFunctionOperation(Input prior, String packageName, TransformDefinition definition) {

        FieldList fieldsToApply = new FieldList(definition.arguments.values().toArray(new String[] {}));
        FieldMetadata targetField = new FieldMetadata(definition.output, definition.type.type());

        RealTimeTransform transform;
        try {
            Class<RealTimeTransform> c = (Class<RealTimeTransform>) Class.forName(packageName + "." + definition.name);
            Constructor<RealTimeTransform> ctor = c.getConstructor();
            transform = ctor.newInstance();
        } catch (Exception e1) {
            log.error(e1);
            throw new RuntimeException(e1);
        }

        Attribute attr = transform.getMetadata();
        AttributeUtils.setFieldMetadataFromAttribute(attr, targetField, false);
        // For now, assume that all Java functions are to be used within RTS
        setRTSProperties(targetField, definition.name, definition.arguments);
        Operation base = new FunctionOperation(
                prior, //
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
