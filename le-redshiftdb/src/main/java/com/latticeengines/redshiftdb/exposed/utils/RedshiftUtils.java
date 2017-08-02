package com.latticeengines.redshiftdb.exposed.utils;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import org.apache.avro.Schema;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
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
        Set<String> keys = new HashSet<>();
        if (StringUtils.isNotBlank(redshiftTableConfig.getDistKey())) {
            keys.add(redshiftTableConfig.getDistKey());
        }
        if (redshiftTableConfig.getSortKeys() != null && !redshiftTableConfig.getSortKeys().isEmpty()) {
            keys.addAll(redshiftTableConfig.getSortKeys());
        }
        StringBuffer sb = new StringBuffer();
        sb.append(String.format( //
                "CREATE TABLE IF NOT EXISTS %s (%s)", //
                redshiftTableConfig.getTableName(), //
                String.join( //
                        ",", //
                        schema.getFields().stream() //
                                .map(field -> getColumnSQLStatement(field, keys)) //
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

    private static String getColumnSQLStatement(Schema.Field field, Set<String> keys) {
        return String.format("\"%s\" %s", field.name(), getSQLType(field, !keys.contains(field.name())));
    }

    public static String getSQLType(Schema.Field field, boolean encode) {
        Schema.Type type = AvroUtils.getType(field);
        StringBuilder sb = new StringBuilder();
        switch (type) {
        case BOOLEAN:
            sb.append("BOOLEAN");
            encode = false;
            break;
        case STRING:
            sb.append("NVARCHAR(1000)");
            break;
        case INT:
            sb.append("INT");
            break;
        case LONG:
            sb.append("BIGINT");
            break;
        case FLOAT:
        case DOUBLE:
            sb.append("FLOAT");
            encode = false;
            break;
        default:
            throw new RuntimeException(String.format("Unsupported avro type %s", type));
        }
        if (encode) {
            if (Schema.Type.FLOAT.equals(type) || Schema.Type.DOUBLE.equals(type)) {
                sb.append(" ENCODE bytedict");
            } else {
                sb.append(" ENCODE lzo");
            }
        }
        return sb.toString();
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

    public static String prependTenantToTableName(CustomerSpace customerSpace, String tableName) {
        String tenant = CustomerSpace.parse(customerSpace.getTenantId()).equals(customerSpace)
                ? customerSpace.getTenantId() : customerSpace.toString();
        return tenant + "_" + tableName;
    }

    public static String extractTenantFromTableName(String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return null;
        } else {
            return tableName.substring(0, tableName.indexOf("_"));
        }
    }

}
