package com.latticeengines.redshiftdb.exposed.utils;

import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.collections.CollectionUtils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;

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

    public static String getCreateTableStatement(RedshiftTableConfiguration redshiftTableConfig, Schema schema) {
        String statement = String.format( //
                "CREATE TABLE IF NOT EXISTS %s (%s)", //
                redshiftTableConfig.getTableName(), //
                String.join( //
                        ",", //
                        schema.getFields().stream() //
                                .map(RedshiftUtils::getColumnSQLStatement) //
                                .collect(Collectors.toList())));

        if (redshiftTableConfig.getDistStyle() != null) {
            statement = String.format("%s diststyle %s", statement, redshiftTableConfig.getDistStyle().getName());
        }
        if (redshiftTableConfig.getDistStyle() == DistStyle.Key && redshiftTableConfig.getDistKey() != null) {
            statement = String.format("%s distkey (%s)", statement, String.join(",", redshiftTableConfig.getDistKey()));
        }
        if (CollectionUtils.isNotEmpty(redshiftTableConfig.getSortKeys())) {
            statement = String.format("%s %s sortkey (%s)", statement, redshiftTableConfig.getSortKeyType().getName(),
                    String.join(",", redshiftTableConfig.getSortKeys()));
        }
        return statement;
    }

    public static String getColumnSQLStatement(Schema.Field field) {
        return String.format("\"%s\" %s", field.name(), getSQLType(field.schema()));
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
