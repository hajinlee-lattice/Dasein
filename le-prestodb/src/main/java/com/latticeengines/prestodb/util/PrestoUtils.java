package com.latticeengines.prestodb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public final class PrestoUtils {

    private static final Logger log = LoggerFactory.getLogger(PrestoUtils.class);

    protected PrestoUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getCheckTableStmt(String catalog, String schema, String tableName) {
        String stmt = "SELECT EXISTS (";
        stmt += "\nSELECT 1 FROM information_schema.tables";
        stmt += String.format("\nWHERE table_catalog = '%s'", catalog);
        stmt += String.format("\nAND table_schema = '%s'", schema);
        stmt += String.format("\nAND table_name = '%s')", tableName.toLowerCase());
        return stmt;
    }

    public static String getDeleteTableStmt(String tableName) {
        return String.format("DROP TABLE IF EXISTS %s", tableName);
    }

    public static String getCreateAvroTableStmt(String tableName, List<Pair<String, Class<?>>> fields, //
                                                List<Pair<String, Class<?>>> partitionKeys, //
                                                String avroDir, String avscPath) {
        String stmt = getCreateTableStmtPrefix(tableName, fields, partitionKeys);
        if (CollectionUtils.isEmpty(partitionKeys)) {
            stmt += String.format("format = 'AVRO', external_location = '%s', avro_schema_url='%s')", //
                    avroDir, avscPath);
        } else {
            stmt += String.format("format = 'AVRO', external_location = '%s')", avroDir);
        }
        return stmt;
    }

    public static String getCreateParquetTableStmt(String tableName, List<Pair<String, Class<?>>> fields, //
                                                   List<Pair<String, Class<?>>> partitionKeys, //
                                                   String parquetDir) {
        String stmt = getCreateTableStmtPrefix(tableName, fields, partitionKeys);
        stmt += String.format("format = 'PARQUET', external_location = '%s')", parquetDir);
        return stmt;
    }

    private static String getCreateTableStmtPrefix(String tableName, List<Pair<String, Class<?>>> fields, //
                                                   List<Pair<String, Class<?>>> partitionKeys) {
        String stmt = String.format("CREATE TABLE IF NOT EXISTS %s (", tableName);
        List<String> fieldLines = getFieldLines(fields, partitionKeys);
        stmt += StringUtils.join(fieldLines, ",");
        stmt += "\n) WITH (";
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            List<String> keyNames = partitionKeys.stream().map(Pair::getLeft).collect(Collectors.toList());
            stmt += "partitioned_by=ARRAY['" + StringUtils.join(keyNames, "', '") +  "'], ";
        }

        return stmt;
    }

    private static List<String> getFieldLines(List<Pair<String, Class<?>>> fields, //
                                              List<Pair<String, Class<?>>> partitionKeys) {
        List<String> fieldLines = new ArrayList<>();
        List<Pair<String, Class<?>>> allFields = new ArrayList<>(fields);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            allFields.addAll(partitionKeys);
        }
        for (Pair<String, Class<?>> pair: allFields) {
            String line = pair.getLeft() + " ";
            switch (pair.getRight().getSimpleName().toLowerCase()) {
                case "string":
                    line += "VARCHAR";
                    break;
                case "integer":
                    line += "INT";
                    break;
                case "long":
                    line += "BIGINT";
                    break;
                case "float":
                    line += "REAL";
                    break;
                case "double":
                    line += "DOUBLE";
                    break;
                case "boolean":
                    line += "BOOLEAN";
                    break;
                case "date":
                    line += "DATE";
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown field class " + pair.getRight());
            }
            fieldLines.add(line);
        }
        return fieldLines;
    }

    public static String getSyncPartitionStmt(String schema, String tableName) {
        return String.format("CALL system.sync_partition_metadata('%s', '%s', 'FULL')", schema, tableName);
    }

    public static DataUnit.DataFormat parseDataFormat(String createStmt) {
        Pattern pattern = Pattern.compile("format = '(?<format>\\w+)'");
        Matcher matcher = pattern.matcher(createStmt);
        if (matcher.find()) {
            String format = matcher.group("format");
            if ("AVRO".equalsIgnoreCase(format)) {
                return DataUnit.DataFormat.AVRO;
            } else if ("PARQUET".equalsIgnoreCase(format)) {
                return DataUnit.DataFormat.PARQUET;
            } else {
                throw new UnsupportedOperationException("Unknown data type " + format);
            }
        } else {
            log.warn("Cannot parse data format from create statement: {}", createStmt);
            return null;
        }
    }

    public static String parseAvscPath(String createStmt) {
        Pattern pattern = Pattern.compile("avro_schema_url = '(?<path>.*\\.avsc)'");
        Matcher matcher = pattern.matcher(createStmt);
        if (matcher.find()) {
            return matcher.group("path");
        } else {
            log.warn("Cannot parse avsc path from create statement: {}", createStmt);
            return null;
        }
    }

}
