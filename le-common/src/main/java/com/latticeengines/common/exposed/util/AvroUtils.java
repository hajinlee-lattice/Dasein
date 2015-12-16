package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.ModifiableRecordBuilder;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AvroUtils {

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

    public static Schema getSchema(Configuration config, Path path) {
        try (FileReader<GenericRecord> reader = getAvroFileReader(config, path)) {
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
        Set<Schema> schemas = new HashSet<>();
        for (String match : matches) {
            schemas.add(AvroUtils.getSchema(config, new Path(match)));
        }
        if (schemas.size() != 1) {
            throw new RuntimeException(String.format("All avro schemas in file glob %s must be the same", path));
        }

        return (Schema) schemas.toArray()[0];
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

    private static FieldAssembler<Schema> constructFieldWithType(FieldAssembler<Schema> fieldAssembler,
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

    private static Type getType(Field field) {
        return field.schema().getTypes().get(0).getType();
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

    public static Type getAvroType(Class<?> javaType) {
        if (javaType == null) {
            return null;
        }
        switch (javaType.getSimpleName()) {
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
        default:
            throw new RuntimeException("Unknown avro type for java type " + javaType.getSimpleName());
        }

    }

    public static String generateHiveCreateTableStatement(String tableName, String pathDir, Schema schema) {
        Schema simplified = extractTypeInformation(schema);

        String template = "CREATE EXTERNAL TABLE %s COMMENT \"%s\"" + //
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" + //
                " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'" + //
                " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" + //
                " LOCATION '%s'" + " TBLPROPERTIES ('avro.schema.literal'='%s')";
        return String.format(template, tableName, "Auto-generated table from metadata service.", pathDir, simplified);
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

    public static void writeToLocalFile(Schema schema, List<GenericRecord> data, String path) throws IOException {
        File avroFile = new File(path);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());) {
            writer.create(schema, avroFile);
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

    public static Schema readSchemaFromLocalFile(String path) throws IOException {
        Schema schema = null;
        try (FileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(new File(path),
                new GenericDatumReader<GenericRecord>())) {
            schema = reader.getSchema();
        }
        return schema;
    }
}
