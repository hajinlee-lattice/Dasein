package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.JythonFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class JythonFunctionOperation extends Operation {
    private static JythonEngine engine = new JythonEngine(null);

    public JythonFunctionOperation(Input prior, String packageName, String moduleName, String functionName,
            FieldList fieldsToApply, FieldMetadata targetField) {

        @SuppressWarnings("unchecked")
        Map<String, String> properties = (Map<String, String>) engine.invoke(packageName, moduleName, "metadata",
                new Object[] {}, Map.class);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("type")) {
                continue;
            }
            targetField.setPropertyValue(entry.getKey(), entry.getValue());
        }
        // For now, assume that all Jython functions are to be used within RTS
        setRTSProperties(targetField, moduleName, fieldsToApply);

        Operation base = new FunctionOperation(prior, //
                new JythonFunction(packageName, //
                        moduleName, //
                        functionName, //
                        DataFlowUtils.convertToFields(fieldsToApply.getFields()), //
                        DataFlowUtils.convertToFields(targetField.getFieldName()), //
                        targetField.getJavaType()), //
                fieldsToApply, //
                targetField, null);

        this.metadata = base.getOutputMetadata();
        this.pipe = base.getOutputPipe();
    }

    private void setRTSProperties(FieldMetadata targetField, String moduleName, FieldList fieldsToApply) {
        // Add as an RTS function
        targetField.setPropertyValue("RTSAttribute", "true");
        targetField.setPropertyValue("RTSModuleName", moduleName);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> args = new HashMap<>();
        String[] fields = fieldsToApply.getFields();
        if (fields.length == 1) {
            args.put("column", fieldsToApply.getFields()[0]);
        } else {
            int i = 1;
            for (String field : fields) {
                args.put("column" + i++, field);
            }
        }
        try {
            targetField.setPropertyValue("RTSArguments", mapper.writeValueAsString(args));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
