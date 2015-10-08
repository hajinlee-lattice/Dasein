package com.latticeengines.eai.service.impl.marketo.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.eai.service.impl.AvroTypeConverter;

@Component("marketoToAvroTypeConverter")
public class MarketoToAvroTypeConverter extends AvroTypeConverter {

    private Map<String, Type> typeMap = new HashMap<String, Type>();

    public MarketoToAvroTypeConverter() {
        typeMap.put("string", Type.STRING);
        typeMap.put("reference", Type.STRING);
        typeMap.put("email", Type.STRING);
        typeMap.put("phone", Type.STRING);
        typeMap.put("url", Type.STRING);
        typeMap.put("text", Type.STRING);
        typeMap.put("id", Type.INT);
        typeMap.put("lead_function", Type.STRING);
        typeMap.put("integer", Type.INT);
        typeMap.put("currency", Type.DOUBLE);
        typeMap.put("boolean", Type.BOOLEAN);
        typeMap.put("date", Type.LONG);
        typeMap.put("datetime", Type.LONG);
    }

    @Override
    public Type convertTypeToAvro(String type) {
        Type avroType = typeMap.get(type);

        if (avroType == null) {
            return super.convertTypeToAvro(type);
        } else {
            return avroType;
        }
    }

}
