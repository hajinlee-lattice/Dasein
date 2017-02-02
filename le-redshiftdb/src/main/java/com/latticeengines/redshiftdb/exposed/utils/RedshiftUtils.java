package com.latticeengines.redshiftdb.exposed.utils;

import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;

public class RedshiftUtils {
    public static final String AVRO_STAGE = "redshift_avro_stage";

    public static void generateJsonPathsFile(Schema schema, OutputStream outputStream) {
        ObjectNode root = JsonUtils.createObjectNode();
        ArrayNode array = root.putArray("jsonpaths");
        for (Schema.Field field : schema.getFields()) {
            array.add(String.format("$.%s", field.name()));
        }
        JsonUtils.serialize(root, outputStream);
    }

    public static String getCreateTableStatement(String tableName, Schema schema) {
        return String.format( //
                "CREATE TABLE %s (%s)", //
                tableName, //
                String.join( //
                        ",", //
                        schema.getFields().stream() //
                                .map(RedshiftUtils::getColumnSQLStatement) //
                                .collect(Collectors.toList())));
    }

    public static String getColumnSQLStatement(Schema.Field field) {
        return String.format("%s %s", field.name(), getSQLType(field.schema()));
    }

    public static String getSQLType(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> innerTypes = schema.getTypes();
            List<Schema> nonNull = innerTypes.stream().filter(t -> !t.getType().equals(Schema.Type.NULL))
                    .collect(Collectors.toList());
            return getSQLType(nonNull.get(0));
        } else {
            switch (schema.getType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "NVARCHAR(MAX)"; // TODO This won't fly
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
            case DOUBLE:
                return "FLOAT";
            default:
                throw new RuntimeException(String.format("Unsupported avro type %s", schema.getType()));
            }
        }
    }
}
