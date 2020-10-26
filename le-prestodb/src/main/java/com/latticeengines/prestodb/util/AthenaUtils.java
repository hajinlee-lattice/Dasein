package com.latticeengines.prestodb.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;

public final class AthenaUtils {

    protected AthenaUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getCheckTableStmt(String schema, String tableName) {
        String stmt = "SELECT EXISTS (";
        stmt += "\nSELECT 1 FROM information_schema.tables";
        stmt += String.format("\nWHERE table_schema = '%s'", schema);
        stmt += String.format("\nAND table_name = '%s')", tableName.toLowerCase());
        return stmt;
    }

    public static String getDeleteTableStmt(String tableName) {
        return String.format("DROP TABLE IF EXISTS %s", tableName);
    }

    public static List<String> getCreateAvroTableStmt(String tableName, Schema schema, //
                                                List<Pair<String, Class<?>>> partitionKeys, //
                                                String avroDir) {
        List<String> stmts = new ArrayList<>(getCreateTableStmts(tableName, schema));
        String firstStmt = stmts.get(0);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            firstStmt += getPartitionStmt(partitionKeys);
        }
        firstStmt += "\nROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'";
        firstStmt += "\nWITH SERDEPROPERTIES ('avro.schema.literal' = '";
        firstStmt += schema.toString(false) + "')";
        firstStmt += "\nSTORED AS AVRO";
        avroDir = avroDir //
                .replace("s3a://", "s3://") //
                .replace("s3n://", "s3://");
        firstStmt += String.format("\nLOCATION '%s'", avroDir);
        stmts.set(0, firstStmt);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            stmts.add(String.format("MSCK REPAIR TABLE `%s`", tableName));
        }
        return stmts;
    }

    public static List<String> getCreateParquetTableStmt(String tableName, Schema schema, //
                                                   List<Pair<String, Class<?>>> partitionKeys, //
                                                   String parquetDir) {
        List<String> stmts = new ArrayList<>(getCreateTableStmts(tableName, schema));
        String firstStmt = stmts.get(0);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            firstStmt += getPartitionStmt(partitionKeys);
        }
        firstStmt += "\nSTORED AS PARQUET";
        parquetDir = parquetDir //
                .replace("s3a://", "s3://") //
                .replace("s3n://", "s3://");
        firstStmt += String.format("\nLOCATION '%s'", parquetDir);
        stmts.set(0, firstStmt);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            stmts.add(String.format("MSCK REPAIR TABLE `%s`", tableName));
        }
        return stmts;
    }

    private static List<String> getCreateTableStmts(String tableName, Schema schema) {
        int chunkSize = 1000;
        List<Pair<String, Class<?>>> fields = AvroUtils.parseSchema(schema);
        List<String> fieldLines = getFieldLines(fields);
        List<String> stmts = new ArrayList<>();
        while (!fieldLines.isEmpty()) {
            List<String> group;
            if (fieldLines.size() > chunkSize) {
                group = fieldLines.subList(0, chunkSize);
                fieldLines = fieldLines.subList(chunkSize, fieldLines.size());
            } else {
                group = new ArrayList<>(fieldLines);
                fieldLines.clear();
            }
            String stmt;
            if (stmts.isEmpty()) {
                stmt = String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s (\n", tableName);
            } else {
                stmt = String.format("ALTER TABLE %s ADD COLUMNS (\n", tableName);
            }
            stmt += StringUtils.join(group, ",\n");
            stmt += "\n)";
            stmts.add(stmt);
        }
        return stmts;
    }

    private static String getPartitionStmt(List<Pair<String, Class<?>>> partitionKeys) {
        String stmt = "\nPARTITIONED BY (\n";
        List<String> fieldLines = getFieldLines(partitionKeys);
        stmt += StringUtils.join(fieldLines, ",\n");
        stmt += "\n)";
        return stmt;
    }

    private static List<String> getFieldLines(List<Pair<String, Class<?>>> fields) {
        List<String> fieldLines = new ArrayList<>();
        for (Pair<String, Class<?>> pair: fields) {
            String line = String.format("`%s` ", pair.getLeft());
            switch (pair.getRight().getSimpleName().toLowerCase()) {
                case "string":
                    line += "STRING";
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

}
