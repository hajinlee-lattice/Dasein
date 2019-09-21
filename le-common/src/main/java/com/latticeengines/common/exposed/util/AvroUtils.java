package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.ModifiableRecordBuilder;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StreamUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.transformer.AvroToCsvTransformer;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

import au.com.bytecode.opencsv.CSVWriter;

public class AvroUtils {

    private static final String SQLSERVER_TYPE_INT = "int";
    private static final String SQLSERVER_TYPE_LONG = "long";
    private static Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    private static Logger log = LoggerFactory.getLogger(AvroUtils.class);
    
    // java type -> (avro schema type, whether java type is primitive)
    // TODO: Java type as Date/Timestamp, List and Map are to be added
    private static final Map<Class<?>, Pair<Type, Boolean>> TYPE_MAP = ImmutableMap
            .<Class<?>, Pair<Type, Boolean>> builder()
            .put(double.class, Pair.of(Type.DOUBLE, Boolean.TRUE)) //
            .put(Double.class, Pair.of(Type.DOUBLE, Boolean.FALSE)) //
            .put(float.class, Pair.of(Type.FLOAT, Boolean.TRUE)) //
            .put(Float.class, Pair.of(Type.FLOAT, Boolean.FALSE)) //
            .put(int.class, Pair.of(Type.INT, Boolean.TRUE)) //
            .put(short.class, Pair.of(Type.INT, Boolean.TRUE)) //
            .put(Integer.class, Pair.of(Type.INT, Boolean.FALSE)) //
            .put(long.class, Pair.of(Type.LONG, Boolean.TRUE)) //
            .put(Long.class, Pair.of(Type.LONG, Boolean.FALSE)) //
            .put(String.class, Pair.of(Type.INT, Boolean.FALSE)) //
            .put(boolean.class, Pair.of(Type.BOOLEAN, Boolean.TRUE)) //
            .put(Boolean.class, Pair.of(Type.BOOLEAN, Boolean.FALSE)) //
            .build();

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

    public static FileReader<GenericRecord> getLocalFileReader(File file) {
        FileReader<GenericRecord> reader;

        try {
            GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<>();
            reader = DataFileReader.openReader(file, fileReader);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Getting avro file reader from path: " + file, e);
        }
        return reader;
    }

    public static DataFileStream<GenericRecord> getAvroFileStream(Configuration config, Path path) {
        DataFileStream<GenericRecord> streamReader;
        try {
            FSDataInputStream input = new FSDataInputStream(HdfsUtils.getInputStream(config, path.toString()));
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
                errorMsgs.add("Found field " + fieldName + " in ordered schema, but not shuffled one.");
            }
        }

        for (String fieldName : fieldMap.keySet()) {
            errorMsgs.add("Found field " + fieldName + " in shuffled schema, but not ordered one.");
        }

        if (!errorMsgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Shuffled and ordered schemas do not match, cannot align.\n" + StringUtils.join(errorMsgs, "\n"));
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
        try (FileReader<GenericRecord> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
            return reader.getSchema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema getSchemaFromGlob(Configuration config, String path) {
        List<String> matches;
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

    public static List<GenericRecord> getDataFromGlob(Configuration configuration, List<String> paths) {
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

    public static Long count(final Configuration configuration, String... globs) {
        List<String> matches = HdfsUtils.getAllMatchedFiles(configuration, globs);
        log.info("Counting " + matches.size() + " avro files at " + StringUtils.join(globs, ","));
        if (matches.size() == 0) {
            throw new IllegalArgumentException("There is no file to be counted.");
        }
        if (matches.size() == 1) {
            return countOneFile(configuration, matches.get(0));
        }
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(4, matches.size()));

        List<Callable<Long>> counters = new ArrayList<>();
        long count = 0L;
        for (int i = 0; i < matches.size(); i++) {
            String match = matches.get(i);
            counters.add(() -> countOneFile(configuration, match));
            if (counters.size() >= 256 || i == matches.size() - 1) {
                List<Long> partialCounts = ThreadPoolUtils.runCallablesInParallel(executorService, counters, 180, 1);
                count += partialCounts.stream().mapToLong(c -> c).sum();
                counters.clear();
            }
        }

        executorService.shutdown();
        log.info(String.format("Totally %d records in %s", count, StringUtils.join(globs, ",")));
        return count;
    }

    private static Long countOneFile(Configuration configuration, String path) {
        // log.info("Counting number of records in " + path);
        Long count = 0L;

        try (DataFileStream<GenericRecord> stream = getAvroFileStream(configuration, new Path(path))) {
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
        return fieldAssembler.endRecord();
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

    public static Schema constructSchemaWithProperties(String tableName, Map<String, Class<?>> classMap,
            Map<String, Map<String, String>> propertyMap) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tableName);
        FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        FieldBuilder<Schema> fieldBuilder;
        for (Map.Entry<String, Class<?>> classEntry : classMap.entrySet()) {
            fieldBuilder = fieldAssembler.name(classEntry.getKey());
            Type type = getAvroType(classEntry.getValue());
            fieldBuilder = constructFieldWithProperties(propertyMap, fieldBuilder, classEntry.getKey());
            fieldAssembler = constructFieldWithType(fieldAssembler, fieldBuilder, type);
        }
        return fieldAssembler.endRecord();
    }

    private static FieldBuilder<Schema> constructFieldWithProperties(Map<String, Map<String, String>> propertyMap,
            FieldBuilder<Schema> fieldBuilder, String fieldName) {
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

    public static FieldAssembler<Schema> constructFieldWithType(FieldAssembler<Schema> fieldAssembler,
            FieldBuilder<Schema> fieldBuilder, Type type) {
        switch (type) {
        case DOUBLE:
            return fieldBuilder.type().unionOf().doubleType().and().nullType().endUnion().noDefault();
        case FLOAT:
            return fieldBuilder.type().unionOf().floatType().and().nullType().endUnion().noDefault();
        case INT:
            return fieldBuilder.type().unionOf().intType().and().nullType().endUnion().noDefault();
        case LONG:
            return fieldBuilder.type().unionOf().longType().and().nullType().endUnion().noDefault();
        case STRING:
            return fieldBuilder.type().unionOf().stringType().and().nullType().endUnion().noDefault();
        case BOOLEAN:
            return fieldBuilder.type().unionOf().booleanType().and().nullType().endUnion().noDefault();
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

    private static void setValues(GenericRecord r, Schema s, Schema combined, ModifiableRecordBuilder recordBldr,
            Map<String, String> nameMap, String nameSuffix) {
        for (Field field : s.getFields()) {
            String key = s.getName() + nameSuffix + "." + field.name();
            Object value = r.get(field.name());
            String combinedSchemaFieldName = nameMap.get(key);
            recordBldr.set(combined.getField(combinedSchemaFieldName), value);
        }
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    public static GenericRecord combineAvroRecords(GenericRecord r1, GenericRecord r2, Object[] schema) {
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

    public static Type convertSqlTypeToAvro(String type) throws IllegalArgumentException, IllegalAccessException {
        // the argument 'type' looks like NVARCHAR(MAX), or NVARCHAR(255), etc.
        String typeStr = StringUtils.substringBefore(type.toLowerCase(), "(");

        if ("DATETIME".equalsIgnoreCase(typeStr)) {
            typeStr = "TIMESTAMP".toLowerCase();
        }

        Map<String, Integer> sqlTypeMap = new HashMap<>();
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
        case "double":
            return Type.DOUBLE;
        case "Float":
        case "float":
            return Type.FLOAT;
        case "Integer":
        case "int":
        case "short":
            return Type.INT;
        case "Long":
        case "long":
            return Type.LONG;
        case "String":
            return Type.STRING;
        case "Boolean":
        case "boolean":
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

    public static String generateHiveCreateTableStatement(String tableName, String pathDir, String schemaHdfsPath) {

        String template = "CREATE EXTERNAL TABLE %s COMMENT \"%s\"" + //
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" + //
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'" + //
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" + //
                " LOCATION '%s'" + " TBLPROPERTIES ('avro.schema.url'='%s')";
        return String.format(template, tableName, "Auto-generated table from metadata service.", pathDir,
                schemaHdfsPath);
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

    public static Schema overwriteFields(Schema schema, Map<String, Schema.Field> fields) {
        List<Schema.Field> newFields = schema.getFields().stream() //
                .map(field -> {
                    Schema.Field srcField = fields.getOrDefault(field.name(), field);
                    Schema.Field newField = new Schema.Field( //
                            field.name(), //
                            field.schema(), //
                            field.doc(), //
                            field.defaultVal() //
                    );
                    Set<String> reserveKeys = Sets.newHashSet("name", "type", "default");
                    srcField.getObjectProps().forEach((k, v) -> {
                        if (!reserveKeys.contains(k)) {
                            newField.addProp(k, v);
                        }
                    });
                    return newField;
                }) //
                .collect(Collectors.toList());
        return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, newFields);
    }

    public static void appendToHdfsFile(Configuration configuration, String filePath, List<GenericRecord> data)
            throws IOException {
        appendToHdfsFile(configuration, filePath, data, false);
    }

    public static void appendToHdfsFile(Configuration configuration, String filePath, List<GenericRecord> data,
            boolean snappy) throws IOException {
        FileSystem fs = HdfsUtils.getFileSystem(configuration, filePath);
        Path path = new Path(filePath);

        if (!HdfsUtils.fileExists(configuration, filePath)) {
            throw new IOException("File " + filePath + " does not exist, so cannot append.");
        }

        try (OutputStream out = fs.append(path)) {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
                if (snappy) {
                    writer.setCodec(CodecFactory.snappyCodec());
                }
                try (DataFileWriter<GenericRecord> appender = writer.appendTo(new FsInput(path, configuration), out)) {
                    for (GenericRecord datum : data) {
                        try {
                            appender.append(datum);
                        } catch (Exception e) {
                            log.warn("Data for the error row: " + datum.toString() + " filePath:" + filePath);
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
            throw new IOException("File " + filePath + " already exists. Please consider using append.");
        }

        try (OutputStream out = fs.create(path)) {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
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

    public static DataFileWriter<GenericRecord> getLocalFileWriter(File avroFile, boolean snappy, boolean create,
            Schema schema) {
        create = !avroFile.exists() || create;
        if (!avroFile.exists() && !create) {

            log.error(avroFile + " does not exist and create == false");
            throw new RuntimeException("try to write to an non-existing file without creating it");

        }

        if (create && schema == null) {

            log.error("try to create " + avroFile + " with schema == null");
            throw new RuntimeException("try to create an avro file without schema");

        }

        DataFileWriter<GenericRecord> writer = null;

        try {
            writer = new DataFileWriter<>(new GenericDatumWriter<>());

            if (snappy) {

                writer.setCodec(CodecFactory.snappyCodec());

            }

            if (create) {

                FileUtils.deleteQuietly(avroFile);
                writer.create(schema, avroFile);

            } else {

                writer.appendTo(avroFile);

            }

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            throw new RuntimeException("getting avro file writer from " + avroFile, e);

        }

        return writer;

    }

    public static void writeToLocalFile(Schema schema, List<GenericRecord> data, String path) throws IOException {
        writeToLocalFile(schema, data, path, false);
    }

    public static void writeToLocalFile(Schema schema, List<GenericRecord> data, String path, boolean snappy)
            throws IOException {
        File avroFile = new File(path);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
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

    public static void appendToLocalFile(List<GenericRecord> data, String path, boolean snappy) throws IOException {
        File avroFile = new File(path);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
            if (snappy) {
                writer.setCodec(CodecFactory.snappyCodec());
            }
            writer.appendTo(avroFile);
            for (GenericRecord datum : data) {
                writer.append(datum);
            }
        }
    }

    public static void appendToLocalFile(String src, String dst, boolean snappy) throws IOException {

        // prepare reader
        try (FileReader<GenericRecord> reader = new DataFileReader<>(new File(src), new GenericDatumReader<>())) {

            // prepare writer
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {

                // writer initialization
                if (snappy) {
                    writer.setCodec(CodecFactory.snappyCodec());
                }
                writer.appendTo(new File(dst));

                // append
                for (GenericRecord datum : reader) {
                    writer.append(datum);
                }
            }
        }
    }

    public static List<GenericRecord> readFromLocalFile(String path) throws IOException {
        List<GenericRecord> data = new ArrayList<>();
        try (FileReader<GenericRecord> reader = new DataFileReader<>(new File(path), new GenericDatumReader<>())) {

            for (GenericRecord datum : reader) {
                data.add(datum);
            }
        }
        return data;
    }

    public static void getAvroFileReader(Configuration config, Path path, File jsonFile,
            Function<GenericRecord, GenericRecord> recProcessor) throws IOException {
        try (FileReader<GenericRecord> reader = getAvroFileReader(config, path)) {
            writeAvroToJsonFile(jsonFile, recProcessor, reader);
        }
    }

    public static void convertAvroToJSON(String avroFilePath, File jsonFile,
            Function<GenericRecord, GenericRecord> recProcessor) throws IOException {
        File avroFile = new File(avroFilePath);
        try (FileReader<GenericRecord> reader = DataFileReader.openReader(avroFile, new GenericDatumReader<>())) {
            writeAvroToJsonFile(jsonFile, recProcessor, reader);
        }
    }

    public static void convertAvroToCSV(String avroFilePath, File csvFile, AvroToCsvTransformer avroToCsvTransformer)
            throws IOException {
        File avroFile = new File(avroFilePath);
        try (FileReader<GenericRecord> reader = DataFileReader.openReader(avroFile, new GenericDatumReader<>())) {
            writeAvroToCsvFile(csvFile, reader, avroToCsvTransformer);
        }
    }

    public static void convertAvroToJSON(Configuration config, String path, File jsonFile,
            Function<GenericRecord, GenericRecord> recProcessor) throws IOException {
        try (AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(config, path)) {
            writeAvroToJsonFile(jsonFile, recProcessor, iterator);
        }
    }

    public static void convertAvroToCSV(Configuration config, String path, File jsonFile,
            AvroToCsvTransformer avroToCsvTransformer) throws IOException {
        try (AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(config, path)) {
            writeAvroToCsvFile(jsonFile, iterator, avroToCsvTransformer);
        }
    }

    public static void writeAvroToJsonFile(File jsonFile, Function<GenericRecord, GenericRecord> recProcessor,
            AvroFilesIterator iterator) throws IOException {
        final GenericData genericData = GenericData.get();
        try (FileOutputStream writer = new FileOutputStream(jsonFile)) {
            writer.write("[".getBytes());
            boolean firstRecord = true;
            while (iterator.hasNext()) {
                GenericRecord currRecord = iterator.next();
                if (currRecord == null) {
                    continue;
                }
                if (!firstRecord) {
                    writer.write(",".getBytes());
                } else {
                    firstRecord = false;
                }
                currRecord = recProcessor != null ? recProcessor.apply(currRecord) : currRecord;
                byte[] bytes = genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8);
                writer.write(bytes);
            }
            writer.write("]".getBytes());
        }
    }

    public static void writeAvroToJsonFile(File jsonFile, Function<GenericRecord, GenericRecord> recProcessor,
            FileReader<GenericRecord> avroReader) throws IOException {
        final GenericData genericData = GenericData.get();
        try (FileOutputStream writer = new FileOutputStream(jsonFile)) {
            writer.write("[".getBytes());
            boolean firstRecord = true;
            while (avroReader.hasNext()) {
                GenericRecord currRecord = avroReader.next();
                if (currRecord == null) {
                    continue;
                }
                if (!firstRecord) {
                    writer.write(",".getBytes());
                } else {
                    firstRecord = false;
                }
                currRecord = recProcessor != null ? recProcessor.apply(currRecord) : currRecord;
                byte[] bytes = genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8);
                writer.write(bytes);
            }
            writer.write("]".getBytes());
        }
    }

    public static void writeAvroToCsvFile(File csvFile, AvroFilesIterator iterator,
            AvroToCsvTransformer avroToCsvTransformer) throws IOException, FileNotFoundException {
        if (avroToCsvTransformer == null) {
            throw new IllegalArgumentException("Cannot convert AVRO CSV. Provide CSV Transformer");
        }
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFile))) {
            List<String> fieldNames = avroToCsvTransformer.getFieldNames(iterator.getSchema());
            System.out.println(fieldNames);
            csvWriter.writeNext(fieldNames.toArray(new String[0]));
            while (iterator.hasNext()) {
                GenericRecord currRecord = iterator.next();
                if (currRecord == null) {
                    continue;
                }

                List<String[]> records = avroToCsvTransformer.getCsvConverterFunction().apply(currRecord);
                if (records != null && records.size() > 0) {
                    csvWriter.writeAll(records);
                }
            }
        }
    }

    public static void writeAvroToCsvFile(File csvFile, FileReader<GenericRecord> reader,
            AvroToCsvTransformer avroToCsvTransformer) throws IOException, FileNotFoundException {
        if (avroToCsvTransformer == null) {
            throw new IllegalArgumentException("Cannot convert AVRO CSV. Provide CSV Transformer");
        }
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFile))) {
            List<String> fieldNames = avroToCsvTransformer.getFieldNames(reader.getSchema());
            System.out.println(fieldNames);
            csvWriter.writeNext(fieldNames.toArray(new String[0]));
            while (reader.hasNext()) {
                GenericRecord currRecord = reader.next();
                if (currRecord == null) {
                    continue;
                }

                List<String[]> records = avroToCsvTransformer.getCsvConverterFunction().apply(currRecord);
                if (records != null && records.size() > 0) {
                    csvWriter.writeAll(records);
                }
            }
        }
    }

    public static long countLocalDir(String dir) {
        Collection<File> files = FileUtils.listFiles(new File(dir), new String[] { "avro" }, false);
        List<Callable<Long>> callables = new ArrayList<>();
        files.forEach(file -> {
            Callable<Long> callable = () -> {
                RetryTemplate rety = RetryUtils.getRetryTemplate(3);
                return rety.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        log.info("Attempt=" + (ctx.getRetryCount() + 1) + ": retry counting " + file.getAbsolutePath());
                    }
                    return countLocalFile(file);
                });
            };
            callables.add(callable);
        });
        ExecutorService tp = ThreadPoolUtils.getFixedSizeThreadPool("avro-count", Math.min(4, callables.size()));
        try {
            List<Long> counts = ThreadPoolUtils.runCallablesInParallel(tp, callables, 120, 1);
            long total = 0L;
            if (CollectionUtils.isNotEmpty(counts)) {
                for (Long c : counts) {
                    total += c;
                }
            }
            return total;
        } finally {
            tp.shutdown();
        }
    }

    private static long countLocalFile(File file) throws IOException {
        long count = 0L;
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(new SeekableFileInput(file),
                new GenericDatumReader<>())) {
            try {
                while (stream.nextBlock() != null) {
                    count += stream.getBlockCount();
                }
            } catch (NoSuchElementException expected) {
                // skip
            }
        }
        return count;
    }

    public static List<GenericRecord> readFromInputStream(InputStream inputStream) throws IOException {
        List<GenericRecord> data = new ArrayList<>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<>())) {
            for (GenericRecord datum : stream) {
                data.add(datum);
            }
        }
        return data;
    }

    public static List<GenericRecord> readFromInputStream(InputStream inputStream, int offset, int limit)
            throws IOException {
        List<GenericRecord> data = new ArrayList<>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<>())) {
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
        Schema schema;
        try (FileReader<GenericRecord> reader = new DataFileReader<>(new File(path), new GenericDatumReader<>())) {
            schema = reader.getSchema();
        }
        return schema;
    }

    public static Schema readSchemaFromInputStream(InputStream inputStream) throws IOException {
        Schema schema;
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, //
                new GenericDatumReader<>())) {
            schema = stream.getSchema();
            inputStream.close();
        }
        return schema;
    }

    public static Schema readSchemaFromResource(ResourceLoader resourceLoader, String resourcePath) throws IOException {
        Schema schema;
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

    public static boolean hasRecords(Configuration configuration, String path) {
        String glob = PathUtils.toAvroGlob(path);
        try (AvroFilesIterator iterator = iterateAvroFiles(configuration, glob)) {
            return iterator.hasNext();
        }
    }

    @Deprecated
    public static Iterator<GenericRecord> iterator(Configuration configuration, String path) {
        return new AvroFilesIterator(configuration, path);
    }

    public static AvroFilesIterator iterateAvroFiles(Configuration configuration, String... paths) {
        return new AvroFilesIterator(configuration, paths);
    }

    public static AvroFilesIterator iterateAvroFiles(Configuration configuration, Collection<String> paths) {
        try {
            return new AvroFilesIterator(configuration, paths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static AvroStreamsIterator iterateAvroStreams(Iterator<InputStream> streamIter) {
        return new AvroStreamsIterator(streamIter);
    }

    public static class AvroFilesIterator implements AvroRecordIterator {

        private List<String> matchedFiles;
        private Integer fileIdx = 0;
        private FileReader<GenericRecord> reader;
        private Configuration configuration;

        AvroFilesIterator(Configuration configuration, String... paths) {
            matchedFiles = HdfsUtils.getAllMatchedFiles(configuration, paths);
            if (CollectionUtils.isEmpty(matchedFiles)) {
                log.warn("Could not find any avro file that matches the path pattern [" //
                        + StringUtils.join(paths, ",") + "]");
            } else {
                this.configuration = configuration;
                reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
            }
        }

        AvroFilesIterator(Configuration configuration, Collection<String> paths) throws IOException {
            matchedFiles = new ArrayList<>();
            for (String path : paths) {
                matchedFiles.addAll(HdfsUtils.getFilesByGlob(configuration, path));
            }
            if (CollectionUtils.isEmpty(matchedFiles)) {
                log.warn("Could not find any avro file that matches one of the path patterns [ "
                        + StringUtils.join(paths, ", ") + " ]");
            } else {
                this.configuration = configuration;
                reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
            }
        }

        @Override
        public boolean hasNext() {
            if (reader != null) {
                while (!reader.hasNext() && fileIdx < matchedFiles.size() - 1) {
                    fileIdx++;
                    try {
                        reader.close();
                    } catch (IOException e) {
                        log.error("Failed to close avro file reader.", e);
                    }
                    reader = getAvroFileReader(configuration, new Path(matchedFiles.get(fileIdx)));
                }
                return reader.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public GenericRecord next() {
            if (reader != null) {
                return reader.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        public Schema getSchema() {
            if (reader != null) {
                return reader.getSchema();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not applicable to this iterator.");
        }

        @Override
        public void close() {
            if (reader == null) {
                return;
            }
            try {
                reader.close();
            } catch (IOException e) {
                log.error("Failed to close avro file reader.", e);
            } finally {
                reader = null;
            }
        }
    }

    /**
     * Iterator of GenericRecords in avros from input streams
     */
    public static class AvroStreamsIterator implements AvroRecordIterator {

        private Iterator<InputStream> streamIter;
        private InputStream currStream;
        private DataFileStream<GenericRecord> reader;

        AvroStreamsIterator(@NotNull Iterator<InputStream> streamIter) {
            Preconditions.checkNotNull(streamIter);
            this.streamIter = streamIter;
            if (this.streamIter.hasNext()) {
                currStream = this.streamIter.next();
                try {
                    reader = new DataFileStream<>(currStream, new GenericDatumReader<>());
                } catch (IOException e) {
                    throw new RuntimeException("Fail to initialize DataFileStream from InputStream", e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (reader == null) {
                return false;
            }
            if (reader.hasNext()) {
                return true;
            }
            while (!reader.hasNext() && streamIter.hasNext()) {
                closeCurrStream();
                currStream = this.streamIter.next();
                try {
                    reader = new DataFileStream<>(currStream, new GenericDatumReader<>());
                } catch (IOException e) {
                    throw new RuntimeException("Fail to initialize DataFileStream from InputStream", e);
                }
            }
            return reader.hasNext();
        }

        @Override
        public GenericRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return reader.next();
        }

        @Override
        public void close() {
            closeFileReader();
            closeCurrStream();
        }

        private void closeFileReader() {
            if (reader == null) {
                return;
            }
            try {
                reader.close();
            } catch (IOException e) {
                log.error("Failed to close avro reader.", e);
            } finally {
                reader = null;
            }
        }

        private void closeCurrStream() {
            if (currStream == null) {
                return;
            }
            try {
                currStream.close();
            } catch (IOException e) {
                log.error("Failed to close input stream.", e);
            } finally {
                currStream = null;
            }
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
        return String.format(s, p);
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
            log.warn("Type mismatch for column=" + column + " avro type=" + avroType + ", value=" + value);
            value = null;
        }
        return value;
    }

    private static Type getFieldType(Field field) {
        Type fieldType = field.schema().getType();

        // if the field is of type union, we must loop to get the correct type
        // if not then there is only one definition
        if (fieldType == Schema.Type.UNION) {
            for (Schema schema : field.schema().getTypes()) {

                if (!schema.equals(NULL_SCHEMA)) {

                    fieldType = schema.getType();
                    break;
                }

            }
        }

        return fieldType;
    }

    @SuppressWarnings("unused")
    private static boolean getFieldAllowsNull(Field field) {
        Type type = field.schema().getType();

        boolean nullAllowed = false;

        // the null is allowed we have two fields (maybe more): one for
        // the field type and one defining null
        if (type == Schema.Type.UNION) {
            for (Schema schema : field.schema().getTypes()) {

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
            if (StringUtils.isNoneBlank(value.toString())) {
                log.warn("Type mismatch for column=" + column + " avro type=" + avroType + ", value=" + value);
            }
            value = null;
        }
        return value;
    }

    public static void createAvroFileByData(Configuration yarnConfiguration, List<Pair<String, Class<?>>> columns,
            Object[][] data, String avroDir, String avroFile) throws Exception {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (Pair<String, Class<?>> column : columns) {
            schemaMap.put(column.getKey(), column.getValue());
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
        Schema schema = AvroUtils.constructSchema(recordName, columns);
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
        writeToHdfsFile(yarnConfiguration, schema, dirPath + File.separator + fileName, records, true);
    }

    public static boolean isValidColumn(String column) {
        if (StringUtils.isBlank(column)) {
            return false;
        }
        return column.matches("^[A-Za-z\\d][A-Za-z\\d\\_]*$");
    }

    /**
     * Convert java class to avro schema. Support converting following class
     * fields to avro fields, other fields are ignored:
     *
     * 1. Fields in java class type contained in TYPE_MAP
     *
     * 2. Fields in java enum type
     *
     * 3. Fields in customized class type who declares an instance method
     * annotated with @SerializeForAvro and returning string type
     *
     * @param cls:
     *            java class
     * @return: avro schema
     */
    public static Schema classToSchema(Class<?> cls) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (java.lang.reflect.Field field : FieldUtils.getAllFields(cls)) {
            if (!isFieldSerializable(field)) {
                log.debug("Field {} with type {} is not supported in serialization to avro", field.getName(),
                        field.getType().getSimpleName());
                continue;
            }
            if (TYPE_MAP.containsKey(field.getType())) {
                columns.add(Pair.of(field.getName(), field.getType()));
            } else {
                columns.add(Pair.of(field.getName(), String.class));
            }
        }
        return constructSchema(cls.getSimpleName(), columns);
    }

    /**
     * Serialize list of java objects to list of generic records for avro.
     * Support converting following class fields to avro fields, other fields
     * are ignored:
     *
     * 1. Fields in java class type contained in TYPE_MAP
     *
     * 2. Fields in java enum type
     *
     * 3. Fields in customized class type who declares an INSTANCE method
     * annotated with @SerializeForAvro, returning string type and without
     * parameter
     *
     * @param cls:
     *            java class
     * @param objects:
     *            list of java objects
     * @return: list of generic records for avro
     */
    public static <T> List<GenericRecord> serialize(Class<T> cls, List<T> objects) {
        Schema schema = classToSchema(cls);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        return objects.stream().map(object -> {
            for (java.lang.reflect.Field field : FieldUtils.getAllFields(cls)) {
                if (!isFieldSerializable(field)) {
                    log.debug("Field {} with type {} is not supported in serialization to avro", field.getName(),
                            field.getType().getSimpleName());
                    continue;
                }
                try {
                    Object value = FieldUtils.readField(field, object, true);
                    if (TYPE_MAP.containsKey(field.getType()) || value == null) {
                        builder.set(field.getName(), value);
                        continue;
                    }
                    if (field.getType().isEnum()) {
                        builder.set(field.getName(), ((Enum<?>) value).name());
                        continue;
                    }
                    // Validation of existence of serializeMethod is in
                    // isFieldSerializable()
                    Method serializeMethod = Arrays.stream(field.getType().getDeclaredMethods()) //
                            .filter(method -> !Modifier.isStatic(method.getModifiers())
                                    && method.isAnnotationPresent(SerializeForAvro.class)) //
                            .findFirst().get();
                    builder.set(field.getName(), serializeMethod.invoke(value));
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException(
                            "Fail to convert java object in type of " + cls.getSimpleName() + " to generic record", e);
                }
            }
            return builder.build();
        }).collect(Collectors.toList());
    }

    /**
     * Whether a field in java class is serializable to an avro field
     *
     * @param field
     * @return
     */
    private static boolean isFieldSerializable(java.lang.reflect.Field field) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
            return false;
        }
        if (TYPE_MAP.containsKey(field.getType())) {
            return true;
        }
        if (field.getType().isEnum()) {
            return true;
        }
        int nSerializeMethod = 0;
        for (Method method : field.getType().getDeclaredMethods()) {
            if (!Modifier.isStatic(method.getModifiers()) //
                    && method.isAnnotationPresent(SerializeForAvro.class) //
                    && method.getReturnType() == String.class //
                    && method.getParameterCount() == 0) {
                nSerializeMethod++;
            }
        }
        return nSerializeMethod == 1;
    }

    /**
     * De-serialize an generic record to a java object with specified type
     * Support converting avro fields to following class fields, otherwise
     * ignore the field:
     *
     * 1. Fields in java class type contained in TYPE_MAP (if java class is
     * primitive, value cannot be null)
     *
     * 2. Fields in java enum type
     *
     * 3. Fields in customized class type who declares a STATIC method annotated
     * with @DeserializeForAvro, returning field's type and taking and only
     * taking single string parameter
     *
     * @param record:
     *            generic record
     * @param cls:
     *            java class
     * @return: java object
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends Object> T deserialize(GenericRecord record, Class<T> cls) {
        T obj = null;
        try {
            obj = cls.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Fail to instantiate class " + cls.getSimpleName(), e);
        }
        for (java.lang.reflect.Field field : FieldUtils.getAllFields(cls)) {
            if (!isFieldDeserializable(field)) {
                log.debug("Field {} with type {} is not supported in de-serialization to avro", field.getName(),
                        field.getType().getSimpleName());
                continue;
            }
            try {
                Object value = record.get(field.getName());
                if (value instanceof Utf8) {
                    value = value.toString();
                }
                if (value == null && field.getType().isPrimitive()) {
                    throw new RuntimeException(String.format("Attempted to set null to field %s with primitive type %s",
                            field.getName(), field.getType().getSimpleName()));
                }
                if (TYPE_MAP.containsKey(field.getType()) || value == null) {
                    FieldUtils.writeField(field, obj, value, true);
                    continue;
                }
                if (field.getType().isEnum()) {
                    value = Enum.valueOf((Class<? extends Enum>) field.getType(), value.toString());
                    FieldUtils.writeField(field, obj, value, true);
                    continue;
                }
                Method deserializeMethod = Arrays.stream(field.getType().getDeclaredMethods()) //
                        .filter(method -> Modifier.isStatic(method.getModifiers())
                                && method.isAnnotationPresent(DeserializeFromAvro.class)) //
                        .findFirst().get();
                FieldUtils.writeField(field, obj, deserializeMethod.invoke(null, value.toString()), true);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RuntimeException(
                        "Fail to convert generic record to java object in type of " + cls.getSimpleName(), e);
            }
        }
        return obj;
    }

    /**
     * Whether a java class field is de-serializable from an avro field
     *
     * @param field
     * @return
     */
    private static boolean isFieldDeserializable(java.lang.reflect.Field field) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
            return false;
        }
        if (TYPE_MAP.containsKey(field.getType())) {
            return true;
        }
        if (field.getType().isEnum()) {
            return true;
        }
        int nDeserializeMethod = 0;
        for (Method method : field.getType().getDeclaredMethods()) {
            if (Modifier.isStatic(method.getModifiers()) //
                    && method.isAnnotationPresent(DeserializeFromAvro.class) //
                    && method.getReturnType() == field.getType() //
                    && method.getParameterCount() == 1 //
                    && method.getParameterTypes()[0] == String.class) {
                nDeserializeMethod++;
            }
        }
        return nDeserializeMethod == 1;
    }
}
