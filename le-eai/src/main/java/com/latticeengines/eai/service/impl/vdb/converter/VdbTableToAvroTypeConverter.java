package com.latticeengines.eai.service.impl.vdb.converter;

import com.latticeengines.eai.service.impl.AvroTypeConverter;
import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component("vdbTableToAvroTypeConverter")
public class VdbTableToAvroTypeConverter extends AvroTypeConverter {
    private Map<String, Type> typeMap = new HashMap<String, Type>();

    public VdbTableToAvroTypeConverter() {
        typeMap.put("bit", Type.BOOLEAN);
        typeMap.put("byte", Type.INT);
        typeMap.put("short", Type.INT);
        typeMap.put("int", Type.INT);
        typeMap.put("long", Type.LONG);
        typeMap.put("float", Type.FLOAT);
        typeMap.put("double", Type.DOUBLE);
        typeMap.put("date", Type.LONG);
        typeMap.put("datetime", Type.LONG);
        typeMap.put("datetimeoffset", Type.LONG);
    }

    @Override
    public Type convertTypeToAvro(String type) {
        if (type.startsWith("varchar") || type.startsWith("nvarchar")) {
            return Type.STRING;
        }
        Type avroType = typeMap.get(type);

        if (avroType == null) {
            return super.convertTypeToAvro(type);
        }
        return avroType;

    }


}
