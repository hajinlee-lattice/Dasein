package com.latticeengines.propdata.eai.service.impl;

import java.util.EnumMap;

import org.apache.avro.Schema.Type;

public class AvroToDBTypeMapper {
    private static EnumMap<Type, String> typeMap = new EnumMap<>(Type.class);
    static {
        typeMap.put(Type.STRING, "VARCHAR");
        typeMap.put(Type.NULL, "NULL");
        typeMap.put(Type.INT, "INT");
        typeMap.put(Type.LONG, "BIGINT");
        typeMap.put(Type.FLOAT, "FLOAT");
        typeMap.put(Type.DOUBLE, "FLOAT");
        typeMap.put(Type.BOOLEAN, "BIT");
        typeMap.put(Type.BYTES, "VARBINARY");
        typeMap.put(Type.BOOLEAN, "BIT");
    }

    public static String getType(Type type) {
        String mappedType = typeMap.get(type);
        if (mappedType == null) {
            mappedType = "VARCHAR";
        }
        return mappedType;
    }
}
