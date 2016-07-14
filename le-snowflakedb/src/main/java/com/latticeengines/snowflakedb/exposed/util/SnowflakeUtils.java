package com.latticeengines.snowflakedb.exposed.util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

import com.latticeengines.common.exposed.util.AvroUtils;


public final class SnowflakeUtils {

    public static final String AVRO_COLUMN = "RECORD";
    public static final String AVRO_RAW_SUFFIX = "_RAW";
    public static final String DEFAULT_SCHEMA = "PUBLIC";

    public static String toQualified(String db, String obj) {
        return String.format("%s.%s.%s", db, SnowflakeUtils.DEFAULT_SCHEMA, obj);
    }

    public static String toAvroRawTable(String table) {
        return table + AVRO_RAW_SUFFIX;
    }

    public static String schemaToView(Schema schema) {
        return schemaToView(schema, null);
    }

    public static String schemaToView(Schema schema, List<String> columnsToExpose) {
        List<Schema.Field> fields = schema.getFields();
        List<Map.Entry<String, String>> columns = new ArrayList<>();

        Set<String> columnSet = new HashSet<>();
        if (columnsToExpose != null && !columnsToExpose.isEmpty()) {
            for (String column : columnsToExpose) {
                columnSet.add(column.toUpperCase());
            }
        }

        for (Schema.Field field: fields) {
            String key = field.name();
            if (columnSet.isEmpty() || columnSet.contains(key.toUpperCase())) {
                Schema.Type avroType = AvroUtils.getType(field);
                String dataType = toSnowflakeType(avroType);
                Map.Entry<String, String> column = new AbstractMap.SimpleEntry<>(key, dataType);
                columns.add(column);
            }
        }

        StringBuilder sb = new StringBuilder();
        List<String> tokens = new ArrayList<>();
        for (Map.Entry<String, String> entry: columns) {
            String token = String.format("    %s:%s::%s AS %s", AVRO_COLUMN, entry.getKey(), entry.getValue(), entry.getKey());
            tokens.add(token);
        }
        sb.append(StringUtils.join(tokens, ",\n")).append("\n");

        return sb.toString();
    }

    private static String toSnowflakeType(Schema.Type type) {
        switch (type) {
            case STRING:
            case BYTES:
                return "VARCHAR";
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case NULL:
            default:
                throw new RuntimeException("Can't convert specified avro schema type to snowflake data type: " + type);
        }
    }

}
