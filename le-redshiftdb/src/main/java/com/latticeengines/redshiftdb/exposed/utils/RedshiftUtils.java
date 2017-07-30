package com.latticeengines.redshiftdb.exposed.utils;

import java.io.OutputStream;
import java.util.Arrays;
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
        StringBuffer sb = new StringBuffer();
        sb.append(String.format( //
                "CREATE TABLE IF NOT EXISTS %s (%s)", //
                redshiftTableConfig.getTableName(), //
                String.join( //
                        ",", //
                        schema.getFields().stream() //
                                .map(RedshiftUtils::getColumnSQLStatement) //
                                .collect(Collectors.toList()))));

        if (redshiftTableConfig.getDistStyle() != null) {
            sb.append(String.format(" diststyle %s", redshiftTableConfig.getDistStyle().getName()));
        }
        if (redshiftTableConfig.getDistStyle() == DistStyle.Key && redshiftTableConfig.getDistKey() != null) {
            sb.append(String.format(" distkey (%s)", String.join(",", redshiftTableConfig.getDistKey())));
        }
        if (CollectionUtils.isNotEmpty(redshiftTableConfig.getSortKeys())) {
            sb.append(String.format(" %s sortkey (%s)", redshiftTableConfig.getSortKeyType().getName(),
                    String.join(",", redshiftTableConfig.getSortKeys())));
        }
        return sb.append(";").toString();
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
                return "NVARCHAR(1000)";
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

    public static String dropTableStatement(String tableName) {
        return String.format("DROP TABLE IF EXISTS %s;", tableName);
    }

    public static String createStagingTableStatement(String stageTableName, String targetTableName) {
        return String.format("CREATE TABLE %s (LIKE %s);", stageTableName, targetTableName);
    }

    public static String renameTableStatement(String originalTableName, String newTableName) {
        return String.format("ALTER TABLE %s RENAME TO %s;", originalTableName, newTableName);
    }

    public static String updateExistingRowsFromStagingTableStatement(String stageTableName, String targetTableName,
            String... joinFields) {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("DELETE FROM %s USING %s WHERE %s; ", targetTableName, stageTableName,
                getJoinStatement(stageTableName, targetTableName, joinFields)));
        sb.append(String.format("INSERT INTO %s SELECT * FROM %s; ", targetTableName, stageTableName));
        return sb.toString();
    }

    private static String getJoinStatement(String stageTableName, String targetTableName, String... joinFields) {
        return Arrays.asList(joinFields).stream().map(f -> {
            return String.format("%1$s.%3$s = %2$s.%3$s", stageTableName, targetTableName, f).toString();
        }).reduce((e1, e2) -> {
            return e1 + " AND " + e2;
        }).orElse(null);
    }
}
