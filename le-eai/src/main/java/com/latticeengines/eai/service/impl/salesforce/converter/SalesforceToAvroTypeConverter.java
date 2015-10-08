package com.latticeengines.eai.service.impl.salesforce.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.eai.service.impl.AvroTypeConverter;

@Component("salesforceToAvroTypeConverter")
public class SalesforceToAvroTypeConverter extends AvroTypeConverter {

    private Map<String, Type> typeMap = new HashMap<String, Type>();

    public SalesforceToAvroTypeConverter() {
        typeMap.put("string", Type.STRING);
        typeMap.put("reference", Type.STRING);
        typeMap.put("email", Type.STRING);
        typeMap.put("phone", Type.STRING);
        typeMap.put("url", Type.STRING);
        typeMap.put("textarea", Type.STRING);
        typeMap.put("id", Type.STRING);
        typeMap.put("picklist", Type.STRING);
        typeMap.put("int", Type.INT);
        typeMap.put("currency", Type.DOUBLE);
        typeMap.put("percent", Type.DOUBLE);
        typeMap.put("boolean", Type.BOOLEAN);
        typeMap.put("date", Type.LONG);
        typeMap.put("datetime", Type.LONG);
    }

    @Override
    public Type convertTypeToAvro(String type) {
        Type avroType = typeMap.get(type);

        if (avroType == null) {
            return super.convertTypeToAvro(type);
        }
        return avroType;

    }

}
