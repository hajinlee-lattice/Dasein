package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.ModifiableRecordBuilder;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.StreamUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AvroUtils {

    private static final String SQLSERVER_TYPE_INT = "int";
    private static final String SQLSERVER_TYPE_LONG = "long";
    private static Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    private static Logger log = LoggerFactory.getLogger(AvroUtils.class);

    public static FileReader<GenericRecord> getAvroFileReader(Configuration config, Path path) {
        SeekableInput input;
        FileReader<GenericRecord> reader;
        try {
            input = new FsInput(path, config);
            GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<>();
            reader = DataFileReader.openReader(input, fileReader);
        } catch (IOException e) {
            throw new RuntimeException("Getting avro file reader from path: " + path.toString(), e);
        }
        return reader;
    }

    public static DataFileStream<GenericRecord> getAvroFileStream(Configuration config, Path path) {
        DataFileStream<GenericRecord> streamReader;
        try {
            FSDataInputStream input = new FSDataInputStream(
                    HdfsUtils.getInputStream(config, path.toString()));
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
            streamReader = new DataFileStream<>(input, reader);
        } catch (IOException e) {
            throw new RuntimeException("Getting avro file reader from path: " + path.toString(), e);
        }
        return streamReader;
    }

    public static Schema alignFields(Schema shuffled, Schema ordered) {
        ObjectNode shuffledJson = JsonUtils.deserialize(shuffled.toString(), ObjectNode.class);
        JsonNode orderedJson = JsonUtils.deserialize(ordered.toString(), JsonNode.class);
        ArrayNode shuffledFields = (ArrayNode) shuffledJson.get("fields");
        ArrayNode orderedFields = (ArrayNode) orderedJson.get("fields");
        Map<String, JsonNode> fieldMap = new HashMap<>();
        for (JsonNode field : shuffledFields) {
            fieldMap.put(field.get("name").asText(), field);
        }
        ArrayNode newFields = new ObjectMapper().createArrayNode();
        List<String> errorMsgs = new ArrayList<>();
        for (JsonNode field : orderedFields) {
            String fieldName = field.get("name").asText();
            if (fieldMap.containsKey(fieldName)) {
                newFields.add(field);
                fieldMap.remove(fieldName);
            } else {
                errorMsgs.add(
                        "Found field " + fieldName + " in ordered schema, but not shuffled one.");
            }
        }

        for (String fieldName : fieldMap.keySet()) {
            errorMsgs.add("Found field " + fieldName + " in shuffled schema, but not ordered one.");
        }

        if (!errorMsgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Shuffled and ordered schemas do not match, cannot align.\n"
                            + StringUtils.join(errorMsgs, "\n"));
        }

        shuffledJson.set("fields", newFields);
        return new Schema.Parser().parse(JsonUtils.serialize(shuffledJson));
    }

    public static Schema removeFields(Schema schema, String... fields) {
        ObjectNode json = JsonUtils.deserialize(schema.toString(), ObjectNode.class);
        ArrayNode oldFields = (ArrayNode) json.get("fields");
        ArrayNode newFields = new ObjectMapper().createArrayNode();
        Set<String> toRemove = new HashSet<>(Arrays.asList(fields));
        for (JsonNode field : oldFields) {
            String fieldName = field.get("name").asText();
            if (!toRemove.contains(fieldName)) {
                newFields.add(field);
            }
        }
        json.set("fields", newFields);
        return new Schema.Parser().parse(JsonUtils.serialize(json));
    }

    public static Schema getSchema(Configuration config, Path path) {
        try (FileReader<GenericRecord> reader = getAvroFileReader(config, path)) {
            return reader.getSchema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getSchemaFields(Configuration config, String path) {
        Schema schema = getSchemaFromGlob(config, path);
        List<String> fields = new ArrayList<>();
        for (Field field : schema.getFields()) {
            fields.add(field.name());
        }
        return fields;
    }

    public static Schema getSchema(File file) {
        try (FileReader<GenericRecord> reader = new DataFileReader<>(file,
                new GenericDatumReader<>())) {
            return reader.getSchema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema getSchemaFromGlob(Configuration config, String path) {
        List<String> matches = null;
        try {
            matches = HdfsUtils.getFilesByGlob(config, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (matches.size() == 0) {
            throw new RuntimeException(String.format("No such file could be found: %s", path));
        }
        return AvroUtils.getSchema(config, new Path(matches.get(0)));
    }

    public static List<GenericRecord> getDataFromGlob(Configuration configuration, String path) {
        try {
            List<String> matches = HdfsUtils.getFilesByGlob(configuration, path);
            List<GenericRecord> output = new ArrayList<>();
            for (String match : matches) {
                output.addAll(AvroUtils.getData(configuration, new Path(match)));
            }
            return output;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<GenericRecord> getDataFromGlob(Configuration configuration,
            List<String> paths) {
        try {
            List<GenericRecord> records = new ArrayList<>();
            for (String path : paths) {
                records.addAll(AvroUtils.getDataFromGlob(configuration, path));
            }
            return records;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<GenericRecord> getData(Configuration configuration, List<String> paths) {
        try {
            List<GenericRecord> records = new ArrayList<>();
            for (String path : paths) {
                records.addAll(AvroUtils.getData(configuration, new Path(path)));
            }
            return records;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<GenericRecord> getData(Configuration config, Path path) throws Exception {
        try (FileReader<GenericRecord> reader = getAvroFileReader(config, path)) {
            List<GenericRecord> data = new ArrayList<>();
            for (GenericRecord datum : reader) {
                data.add(datum);
            }
            return data;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long count(final Configuration configuration, String glob) {
        Long count = 0L;
        try {
            List<String> matches = HdfsUtils.getFilesByGlob(configuration, glob);

            log.info("Counting " + matches.size() + " avro files at " + glob);

            if (matches.size() == 0) {
                throw new IllegalArgumentException("There is no file to be counted.");
            }

            if (matches.size() == 1) {
                return countOneFile(configuration, matches.get(0));
            }

            ExecutorService executorService = Executors
                    .newFixedThreadPool(Math.min(8, matches.size()));
            Map<String, Future<Long>> futures = new HashMap<>();
            for (final String match : matches) {
                Future<Long> future = executorService
                        .submit(() -> countOneFile(configuration, match));
                futures.put(match, future);
            }

            for (Map.Entry<String, Future<Long>> entry : futures.entrySet()) {
                String file = entry.getKey();
                Long partialCount;
                try {
                    partialCount = entry.getValue().get();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to count file " + file, e);
                }
                count += partialCount;
            }
            executorService.shutdown();
            log.info(String.format("Totally %d records in %s", count.longValue(), glob));
            return count;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Long countOneFile(Configuration configuration, String path) {
        // log.info("Counting number of records in " + path);
        Long count = 0L;

        try (DataFileStream<GenericRecord> stream = getAvroFileStream(configuration,
                new Path(path));) {
            try {
                while (stream.nextBlock() != null) {
                    count += stream.getBlockCount();
                }
            } catch (NoSuchElementException e) {
                // log.info("Seems no next block in current stream.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to count avro at " + path, e);
        }

        return count;
    }

    @SuppressWarnings("deprecation")
    public static Object[] combineSchemas(Schema s1, Schema s2) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(s1.getName());

        for (Map.Entry<String, String> entry : s1.getProps().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();

            if (k.equals("uuid")) {
                recordBuilder = recordBuilder.prop("uuid", UUID.randomUUID().toString());
            } else {
                recordBuilder = recordBuilder.prop(k, v);
            }
        }

        String s1uuid = s1.getProp("uuids");
        if (s1uuid == null) {
            s1uuid = s1.getProp("uuid");
        }

        String s2uuid = s2.getProp("uuids");
        if (s2uuid == null) {
            s2uuid = s2.getProp("uuid");
        }

        if (s2uuid != null) {
            recordBuilder = recordBuilder.prop("uuids", s1uuid + "," + s2uuid);
        }

        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc(s1.getDoc()).fields();

        FieldBuilder<Schema> fieldBuilder;
        Map<String, String> map = new HashMap<>();
        Set<String> colNames = new HashSet<>();
        List<Field> fields = new ArrayList<>();
        fields.addAll(s1.getFields());
        fields.addAll(s2.getFields());
        int i = 0;
        int cutoff = s1.getFields().size();
        for (Field field : fields) {
            String key = s1.getName() + "$1";
            String colName = field.name();
            if (i >= cutoff) {
                key = s2.getName() + "$2";
            }

            if (colNames.contains(colName)) {
                colName = colName + "_1";
            }

            map.put(key + "." + field.name(), colName);
            colNames.add(colName);
            fieldBuilder = fieldAssembler.name(colName);
            Map<String, String> props = field.props();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                fieldBuilder = fieldBuilder.prop(k, v);
            }

            Type type = getType(field);
            fieldAssembler = constructFieldWithType(fieldAssembler, fieldBuilder, type);

            i++;
        }
        Schema schema = fieldAssembler.endRecord();
        return new Object[] { schema, map };
    }

    public static Schema constructSchema(String tableName, Map<String, Class<?>> classMap) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tableName);
        FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        FieldBuilder<Schema> fieldBuilder;
        for (Map.Entry<String, Class<?>> classEntry : classMap.entrySet()) {
            fieldBuilder = fieldAssembler.name(classEntry.getKey());
            Type type = getAvroType(classEntry.getValue());
            fieldAssembler = constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        Schema schema = fieldAssembler.endRecord();
        return schema;
    }

    public static Schema constructSchema(String tableName, List<Pair<String, Class<?>>> columns) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tableName);
        FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        FieldBuilder<Schema> fieldBuilder;
        for (Pair<String, Class<?>> pair : columns) {
            fieldBuilder = fieldAssembler.name(pair.getLeft());
            Type type = getAvroType(pair.getRight());
            fieldAssembler = constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        return fieldAssembler.endRecord();
    }

    public static Schema constructSchemaWithProperties(String tableName,
            Map<String, Class<?>> classMap, Map<String, Map<String, String>> propertyMap) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tableName);
        FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        FieldBuilder<Schema> fieldBuilder;
        for (Map.Entry<String, Class<?>> classEntry : classMap.entrySet()) {
            fieldBuilder = fieldAssembler.name(classEntry.getKey());
            Type type = getAvroType(classEntry.getValue());
            fieldBuilder = constructFieldWithProperties(propertyMap, fieldBuilder,
                    classEntry.getKey());
            fieldAssembler = constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        Schema schema = fieldAssembler.endRecord();
        return schema;
    }

    private static FieldBuilder<Schema> constructFieldWithProperties(
            Map<String, Map<String, String>> propertyMap, FieldBuilder<Schema> fieldBuilder,
            String fieldName) {
        if (propertyMap != null) {
            Map<String, String> properties = propertyMap.get(fieldName);
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    fieldBuilder = fieldBuilder.prop(k, v);
                }
            }
        }
        return fieldBuilder;
    }

    public static FieldAssembler<Schema> constructFieldWithType(
            FieldAssembler<Schema> fieldAssembler, FieldBuilder<Schema> fieldBuilder, Type type) {
        switch (type) {
            case DOUBLE:
                return fieldBuilder.type().unionOf().doubleType().and().nullType().endUnion()
                        .noDefault();
            case FLOAT:
                return fieldBuilder.type().unionOf().floatType().and().nullType().endUnion()
                        .noDefault();
            case INT:
                return fieldBuilder.type().unionOf().intType().and().nullType().endUnion()
                        .noDefault();
            case LONG:
                return fieldBuilder.type().unionOf().longType().and().nullType().endUnion()
                        .noDefault();
            case STRING:
                return fieldBuilder.type().unionOf().stringType().and().nullType().endUnion()
                        .noDefault();
            case BOOLEAN:
                return fieldBuilder.type().unionOf().booleanType().and().nullType().endUnion()
                        .noDefault();
            default:
                return fieldAssembler;
        }
    }

    public static Type getType(Field field) {
        if (Type.UNION.equals(field.schema().getType())) {
            Type bestType = Type.NULL;
            for (Schema schema : field.schema().getTypes()) {
                Type type = schema.getType();
                if (!Type.NULL.equals(type)) {
                    bestType = type;
                    break;
                }
            }
            return bestType;
        } else {
            return field.schema().getType();
        }
    }

    private static void setValues(GenericRecord r, Schema s, Schema combined,
            ModifiableRecordBuilder recordBldr, Map<String, String> nameMap, String nameSuffix) {
        for (Field field : s.getFields()) {
            String key = s.getName() + nameSuffix + "." + field.name();
            Object value = r.get(field.name());
            String combinedSchemaFieldName = nameMap.get(key);
            recordBldr.set(combined.getField(combinedSchemaFieldName), value);
        }
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    public static GenericRecord combineAvroRecords(GenericRecord r1, GenericRecord r2,
            Object[] schema) {
        Schema s2 = r2.getSchema();
        Schema combinedSchema = Schema.parse((String) schema[0]);
        ModifiableRecordBuilder recordBldr = new ModifiableRecordBuilder(combinedSchema, r1, r2);
        Map<String, String> nameMap = (Map<String, String>) schema[1];
        setValues(r2, s2, combinedSchema, recordBldr, nameMap, "$2");
        return recordBldr.buildCombinedRecord();
    }

    public static String getAvroFriendlyString(String value) {
        return value.replaceAll("[^A-Za-z0-9()\\[\\]]", "_");
    }

    public static boolean isAvroFriendlyFieldName(String fieldName) {
        try {
            Method m = Schema.class.getDeclaredMethod("validateName", String.class);
            m.setAccessible(true);
            m.invoke(null, fieldName);
        } catch (Exception e) {
            log.error(ExceptionUtils.getRootCauseMessage(e));
            return false;
        }
        return true;
    }

    public static Class<?> getJavaType(Type avroType) {
        if (avroType == null) {
            return null;
        }
        switch (avroType) {
            case DOUBLE:
                return Double.class;
            case FLOAT:
                return Float.class;
            case INT:
                return Integer.class;
            case LONG:
                return Long.class;
            case STRING:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            default:
                throw new RuntimeException("Unknown java type for avro type " + avroType);
        }
    }

    public static String getHiveType(String avroType) {
        if (avroType == null) {
            return null;
        }
        return getHiveType(Type.valueOf(avroType.toUpperCase()));
    }

    public static String getHiveType(Type avroType) {
        if (avroType == null) {
            return null;
        }
        switch (avroType) {
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case STRING:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case BYTES:
                return "BINARY";

            default:
                throw new RuntimeException("Unknown hive type for avro type " + avroType);
        }

    }

    public static Type convertSqlTypeToAvro(String type)
            throws IllegalArgumentException, IllegalAccessException {
        // the argument 'type' looks like NVARCHAR(MAX), or NVARCHAR(255), etc.
        String typeStr = StringUtils.substringBefore(type.toLowerCase(), "(");

        if ("DATETIME".equalsIgnoreCase(typeStr)) {
            typeStr = "TIMESTAMP".toLowerCase();
        }

        Map<String, Integer> sqlTypeMap = new HashMap<String, Integer>();
        for (java.lang.reflect.Field field : java.sql.Types.class.getFields()) {
            sqlTypeMap.put(field.getName().toLowerCase(), (Integer) field.get(null));
        }
        if (sqlTypeMap.containsKey(typeStr)) {
            int sqlTypeInt = sqlTypeMap.get(typeStr);
            switch (sqlTypeInt) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    return Type.INT;
                case Types.BIGINT:
                    return Type.LONG;
                case Types.BIT:
                case Types.BOOLEAN:
                    return Type.BOOLEAN;
                case Types.REAL:
                    return Type.FLOAT;
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    return Type.DOUBLE;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                    return Type.STRING;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    return Type.LONG;
                case Types.BLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return Type.BYTES;
                default:
                    throw new IllegalArgumentException("Cannot convert SQL type " + typeStr);
            }
        } else {
            // we need to handle SQLSERVER type INT separately as it is not
            // covered by java.sql.Types
            if (SQLSERVER_TYPE_INT.equalsIgnoreCase(typeStr)) {
                return Type.INT;
            } else if (SQLSERVER_TYPE_LONG.equalsIgnoreCase(typeStr)) {
                return Type.LONG;
            }
            throw new IllegalArgumentException("Cannot convert SQL type " + typeStr);
        }
    }

    public static SQLType getSqlType(Class<?> javaClz) {
        return getSqlType(getAvroType(javaClz));
    }

    private static SQLType getSqlType(Type avroType) {
        SQLType type;
        switch (avroType) {
            case BOOLEAN:
                type = JDBCType.BOOLEAN;
                break;
            case STRING:
                type = JDBCType.VARCHAR;
                break;
            case INT:
                type = JDBCType.INTEGER;
                break;
            case LONG:
                type = JDBCType.BIGINT;
                break;
            case FLOAT:
            case DOUBLE:
                type = JDBCType.FLOAT;
                break;
            default:
                throw new RuntimeException(String.format("Unsupported avro type %s", avroType));
        }
        return type;
    }

    public static Type getAvroType(Class<?> javaType) {
        if (javaType == null) {
            return null;
        }
        return getAvroType(javaType.getSimpleName());
    }

    public static Type getAvroType(String javaClassName) {
        if (StringUtils.isEmpty(javaClassName)) {
            return null;
        }
        switch (javaClassName) {
            case "Double":
                return Type.DOUBLE;
            case "Float":
                return Type.FLOAT;
            case "Integer":
                return Type.INT;
            case "Long":
                return Type.LONG;
            case "String":
                return Type.STRING;
            case "Boolean":
                return Type.BOOLEAN;
            case "Date":
                return Type.LONG;
            case "Timestamp":
                return Type.LONG;
            case "List":
                return Type.ARRAY;
            case "Map":
                return Type.RECORD;
            default:
                throw new RuntimeException("Unknown avro type for java type " + javaClassName);
        }

    }

    public static String generateHiveCreateTableStatement(String tableName, String pathDir,
            String schemaHdfsPath) {

        String template = "CREATE EXTERNAL TABLE %s COMMENT \"%s\"" + //
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" + //
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'"
                + //
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" + //
                " LOCATION '%s'" + " TBLPROPERTIES ('avro.schema.url'='%s')";
        return String.format(template, tableName, "Auto-generated table from metadata service.",
                pathDir, schemaHdfsPath);
    }

    public static Schema extractTypeInformation(Schema schema) {
        FieldAssembler<Schema> assembler = SchemaBuilder //
                .record(schema.getName()) //
                .namespace(schema.getNamespace()) //
                .doc(schema.getDoc()) //
                .fields();
        for (Field field : schema.getFields()) {
            FieldBuilder<Schema> fieldBuilder = assembler.name(field.name());
            assembler = constructFieldWithType(assembler, fieldBuilder, getType(field));
        }
        return assembler.endRecord();
    }

    public static Schema extractSimpleSchema(Schema schema) {
        FieldAssembler<Schema> assembler = SchemaBuilder //
                .record(schema.getName()) //
                .doc(schema.getDoc()) //
                .fields();
        for (Field field : schema.getFields()) {
            FieldBuilder<Schema> fieldBuilder = assembler.name(field.name());
            assembler = constructFieldWithType(assembler, fieldBuilder, getType(field));
        }
        return assembler.endRecord();
    }

    public static void appendToHdfsFile(Configuration configuration, String filePath,
            List<GenericRecord> data) throws IOException {
        appendToHdfsFile(configuration, filePath, data, false);
    }

    public static void appendToHdfsFile(Configuration configuration, String filePath,
            List<GenericRecord> data, boolean snappy) throws IOException {
        FileSystem fs = HdfsUtils.getFileSystem(configuration, filePath);
        Path path = new Path(filePath);

        if (!HdfsUtils.fileExists(configuration, filePath)) {
            throw new IOException("File " + filePath + " does not exist, so cannot append.");
        }

        try (OutputStream out = fs.append(path)) {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>())) {
                if (snappy) {
                    writer.setCodec(CodecFactory.snappyCodec());
                }
                try (DataFileWriter<GenericRecord> appender = writer
                        .appendTo(new FsInput(path, configuration), out)) {
                    for (GenericRecord datum : data) {
                        try {
                            appender.append(datum);
                        } catch (Exception e) {
                            log.warn("Data for the error row: " + datum.toString() + " filePath:"
                                    + filePath);
                            throw new IOException(e);
                        }
                    }
                }
            }
        }
    }

    public static void writeToHdfsFile(Configuration configuration, Schema schema, String filePath,
            List<GenericRecord> data) throws IOException {
        writeToHdfsFile(configuration, schema, filePath, data, false);
    }

    public static void writeToHdfsFile(Configuration configuration, Schema schema, String filePath,
            List<GenericRecord> data, boolean snappy) throws IOException {
        FileSystem fs = HdfsUtils.getFileSystem(configuration, filePath);
        Path path = new Path(filePath);

        if (HdfsUtils.fileExists(configuration, filePath)) {
            throw new IOException(
                    "File " + filePath + " already exists. Please consider using append.");
        }

        try (OutputStream out = fs.create(path)) {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>())) {
                if (snappy) {
                    writer.setCodec(CodecFactory.snappyCodec());
                }
                try (DataFileWriter<GenericRecord> creator = writer.create(schema, out)) {
                    for (GenericRecord datum : data) {
                        try {
                            creator.append(datum);
                        } catch (Exception e) {
                            log.warn("Data for the error row: " + datum.toString());
                            throw new IOException(e);
                        }

                    }
                }
            }
        }
    }

    public static void writeToLocalFile(Schema schema, List<GenericRecord> data, String path)
            throws IOException {
        writeToLocalFile(schema, data, path, false);
    }

    public static void writeToLocalFile(Schema schema, List<GenericRecord> data, String path,
            boolean snappy) throws IOException {
        File avroFile = new File(path);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<GenericRecord>());) {
            if (snappy) {
                writer.setCodec(CodecFactory.snappyCodec());
            }
            writer.create(schema, avroFile);
            for (GenericRecord datum : data) {
                writer.append(datum);
            }
        }
    }

    public static void appendToLocalFile(List<GenericRecord> data, String path) throws IOException {
        appendToLocalFile(data, path, false);
    }

    public static void appendToLocalFile(List<GenericRecord> data, String path, boolean snappy)
            throws IOException {
        File avroFile = new File(path);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<GenericRecord>());) {
            if (snappy) {
                writer.setCodec(CodecFactory.snappyCodec());
            }
            writer.appendTo(avroFile);
            for (GenericRecord datum : data) {
                writer.append(datum);
            }
        }
    }

    public static List<GenericRecord> readFromLocalFile(String path) throws IOException {
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        try (FileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(new File(path),
                new GenericDatumReader<GenericRecord>())) {

            for (GenericRecord datum : reader) {
                data.add(datum);
            }
        }
        return data;
    }

    public static List<GenericRecord> readFromInputStream(InputStream inputStream)
            throws IOException {
        List<GenericRecord> data = new ArrayList<GenericRecord>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<GenericRecord>())) {
            for (GenericRecord datum : stream) {
                data.add(datum);
            }
        }
        return data;
    }

    public static List<GenericRecord> readFromInputStream(InputStream inputStream, int offset,
            int limit) throws IOException {
        List<GenericRecord> data = new ArrayList<>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<GenericRecord>())) {
            int count = 0;
            for (GenericRecord datum : stream) {
                if (count++ < offset) {
                    continue;
                }
                if (count > offset + limit) {
                    break;
                }
                data.add(datum);
            }
            inputStream.close();
        }
        return data;
    }

    public static Schema readSchemaFromLocalFile(String path) throws IOException {
        Schema schema = null;
        try (FileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(new File(path),
                new GenericDatumReader<GenericRecord>())) {
            schema = reader.getSchema();
        }
        return schema;
    }

    public static Schema readSchemaFromInputStream(InputStream inputStream) throws IOException {
        Schema schema;
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<GenericRecord>())) {
            schema = stream.getSchema();
            inputStream.close();
        }
        return schema;
    }

    public static Schema readSchemaFromResource(ResourceLoader resourceLoader, String resourcePath)
            throws IOException {
        Schema schema = null;
        Resource schemaResource = resourceLoader.getResource(resourcePath);
        try (InputStream is = schemaResource.getInputStream()) {
            schema = new Schema.Parser().parse(is);
        }
        return schema;
    }

    public static List<GenericRecord> convertToRecords(Object[][] data, Schema schema) {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            int i = 0;
            for (Schema.Field field : schema.getFields()) {
                builder.set(field, tuple[i]);
                i++;
            }
            records.add(builder.build());
        }
        return records;
    }

    public static Iterator<GenericRecord> iterator(Configuration configuration, String path) {
        try {
            return new AvroFilesIterator(configuration, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Iterator<GenericRecord> iterator(Configuration configuration,
            Collection<String> paths) {
        try {
            return new AvroFilesIterator(configuration, paths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class AvroFilesIterator implements Iterator<GenericRecord> {

        private List<String> matchedFiles;
        private Integer fileIdx = 0;
        private FileReader<GenericRecord> reader;
        private Configuration configuration;

        AvroFilesIterator(Configuration configuration, String path) throws IOException {
            matchedFiles = HdfsUtils.getFilesByGlob(configuration, path);
            if (matchedFiles == null || matchedFiles.isEmpty()) {
                throw new IOException("Could not find any avro file that matches the path pattern ["
                        + path + "]");
            }
            this.configuration = configuration;
            reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
        }

        AvroFilesIterator(Configuration configuration, Collection<String> paths)
                throws IOException {
            matchedFiles = new ArrayList<>();
            for (String path : paths) {
                matchedFiles.addAll(HdfsUtils.getFilesByGlob(configuration, path));
            }
            if (matchedFiles == null || matchedFiles.isEmpty()) {
                throw new IOException(
                        "Could not find any avro file that matches one of the path patterns [ "
                                + StringUtils.join(paths, ", ") + " ]");
            }
            this.configuration = configuration;
            reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
        }

        @Override
        public boolean hasNext() {
            while (!reader.hasNext() && fileIdx < matchedFiles.size() - 1) {
                fileIdx++;
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("Failed to close avro file reader.");
                }
                reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
            }
            return reader.hasNext();
        }

        @Override
        public GenericRecord next() {
            return reader.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not applicable to this iterator.");
        }
    }

    public static String buildSchema(String avscFile, Object... params) {
        InputStream is = ClassLoader.getSystemResourceAsStream(avscFile);
        if (is == null) {
            return null;
        }
        String s;
        try {
            s = StreamUtils.copyToString(is, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Object[] p = new Object[params.length];
        int i = 0;
        for (Object o : params) {
            p[i++] = o;
        }
        String schema = String.format(s, p);
        return schema;
    }

    public static Object checkTypeAndConvert(String column, Object value, Type avroType) {
        if (value == null || avroType == null) {
            return value;
        }
        try {
            switch (avroType) {
                case DOUBLE:
                    if (!(value instanceof Double)) {
                        return Double.valueOf(value.toString());
                    }
                    break;
                case FLOAT:
                    if (!(value instanceof Float)) {
                        return Float.valueOf(value.toString());
                    }
                    break;
                case INT:
                    if (!(value instanceof Integer)) {
                        return Integer.valueOf(value.toString());
                    }
                    break;
                case LONG:
                    if (!(value instanceof Long)) {
                        return Long.valueOf(value.toString());
                    }
                    break;
                case BOOLEAN:
                    if (!(value instanceof Boolean)) {
                        return Boolean.valueOf(value.toString());
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception ex) {
            log.warn("Type mismatch for column=" + column + " avro type=" + avroType + ", value="
                    + value);
            value = null;
        }
        return value;
    }

    private static Type getFieldType(Field field)
    {
        Type fieldType = field.schema().getType();

        // if the field is of type union, we must loop to get the correct type
        // if not then there is only one definition
        if(fieldType == Schema.Type.UNION)
        {
            for (Schema schema: field.schema().getTypes()) {

                if (!schema.equals(NULL_SCHEMA)) {

                    fieldType = schema.getType();
                    break;
                }

            }
        }

        return fieldType;
    }

    private static boolean getFieldAllowsNull(Field field)
    {
        Type type = field.schema().getType();

        boolean nullAllowed = false;

        // the null is allowed we have two fields (maybe more): one for
        // the field type and one defining null
        if(type == Schema.Type.UNION)
        {
            for (Schema schema: field.schema().getTypes()) {

                if (schema.equals(NULL_SCHEMA)) {

                    nullAllowed = true;
                    break;

                }

            }
        }

        return nullAllowed;
    }

    public static Object checkTypeAndConvertEx(String column, Object value, Field avroField) {
        Type avroType = getFieldType(avroField);
        if (value == null || avroType == null) {
            return value;
        }
        try {
            switch (avroType) {
                case DOUBLE:
                    if (!(value instanceof Double)) {
                        return Double.valueOf(value.toString());
                    }
                    break;
                case FLOAT:
                    if (!(value instanceof Float)) {
                        return Float.valueOf(value.toString());
                    }
                    break;
                case INT:
                    if (!(value instanceof Integer)) {
                        return Integer.valueOf(value.toString());
                    }
                    break;
                case LONG:
                    if (!(value instanceof Long)) {
                        return Long.valueOf(value.toString());
                    }
                    break;
                case BOOLEAN:
                    if (!(value instanceof Boolean)) {
                        return Boolean.valueOf(value.toString());
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception ex) {
            log.warn("Type mismatch for column=" + column + " avro type=" + avroType + ", value="
                    + value);
            value = null;
        }
        return value;
    }

    public static void createAvroFileByData(Configuration yarnConfiguration,
            List<Pair<String, Class<?>>> columns, Object[][] data, String avroDir, String avroFile)
            throws Exception {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            schemaMap.put(columns.get(i).getKey(), columns.get(i).getValue());
        }
        Schema schema = AvroUtils.constructSchema(avroFile, schemaMap);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            for (int i = 0; i < columns.size(); i++) {
                builder.set(columns.get(i).getKey(), tuple[i]);
            }
            records.add(builder.build());
        }
        String fileName = avroFile;
        if (!fileName.endsWith(".avro"))
            fileName = avroFile + ".avro";
        if (HdfsUtils.fileExists(yarnConfiguration, avroDir + "/" + fileName)) {
            HdfsUtils.rmdir(yarnConfiguration, avroDir + "/" + fileName);
        }
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, avroDir + "/" + fileName, records);
    }

    public static void uploadAvro(Configuration yarnConfiguration, //
            Object[][] data, //
            List<Pair<String, Class<?>>> columns, //
            String recordName, //
            String dirPath) throws Exception {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (Pair<String, Class<?>> column : columns) {
            schemaMap.put(column.getKey(), column.getValue());
        }
        Schema schema = AvroUtils.constructSchema(recordName, schemaMap);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            for (int i = 0; i < columns.size(); i++) {
                builder.set(columns.get(i).getKey(), tuple[i]);
            }
            records.add(builder.build());
        }
        String fileName = recordName + ".avro";
        if (HdfsUtils.fileExists(yarnConfiguration, dirPath)) {
            HdfsUtils.rmdir(yarnConfiguration, dirPath);
        }
        writeToHdfsFile(yarnConfiguration, schema, dirPath + File.separator + fileName, records);
    }

    public static boolean isValidColumn(String column) {
        if (StringUtils.isBlank(column)) {
            return false;
        }
        return column.matches("^[A-Za-z\\d][A-Za-z\\d\\_]*$");
    }
}
