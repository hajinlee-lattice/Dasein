package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class MetadataConverter {
    private static Set<Schema.Type> supportedAvroTypes = new HashSet<Schema.Type>();

    static {
        supportedAvroTypes.add(Schema.Type.STRING);
        supportedAvroTypes.add(Schema.Type.INT);
        supportedAvroTypes.add(Schema.Type.LONG);
        supportedAvroTypes.add(Schema.Type.FLOAT);
        supportedAvroTypes.add(Schema.Type.DOUBLE);
        supportedAvroTypes.add(Schema.Type.BOOLEAN);
    }

    public static Table readMetadataFromAvroFile(Configuration configuration, String path, String primaryKeyName,
            String lastModifiedKeyName) {
        try {
            Schema schema = AvroUtils.getSchemaFromGlob(configuration, path);
            List<Extract> extracts = new ArrayList<Extract>();
            Extract extract = new Extract();
            try (FileSystem fs = FileSystem.newInstance(configuration)) {
                extract.setExtractionTimestamp(fs.getFileStatus(new Path(path)).getModificationTime());
            }
            extract.setName("extract");
            extract.setPath(path);
            extracts.add(extract);
            Table table = convertToTable(schema, extracts, primaryKeyName, lastModifiedKeyName);
            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse metadata for avro file located at %s", path), e);
        }
    }

    public static Table convertToTable(Schema schema, List<Extract> extracts, String primaryKeyName,
            String lastModifiedKeyName) {
        try {
            Table table = new Table();
            table.setName(schema.getName());
            table.setDisplayName(schema.getFullName());

            List<Schema.Field> fields = schema.getFields();

            for (Schema.Field field : fields) {
                Attribute attr = convertToAttribute(field);
                table.addAttribute(attr);
            }

            table.setExtracts(extracts);

            if (primaryKeyName != null) {
                Schema.Field primaryKeyField = schema.getField(primaryKeyName);
                if (primaryKeyField == null) {
                    throw new RuntimeException(String.format("Could not locate primary key field %s in avro schema",
                            primaryKeyName));
                }
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.setName(primaryKeyName);
                primaryKey.addAttribute(primaryKeyName);
                table.setPrimaryKey(primaryKey);
            }

            if (lastModifiedKeyName != null) {
                Schema.Field lastModifiedField = schema.getField(lastModifiedKeyName);
                if (lastModifiedField == null) {
                    throw new RuntimeException(String.format(
                            "Could not locate last modified key field %s in avro schema", lastModifiedKeyName));
                }

                LastModifiedKey lastModifiedKey = new LastModifiedKey();
                lastModifiedKey.setName(lastModifiedKeyName);
                lastModifiedKey.addAttribute(lastModifiedKeyName);
                table.setLastModifiedKey(lastModifiedKey);
            }

            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read avro schema from %s", schema.getName()), e);
        }
    }

    public static Attribute convertToAttribute(Schema.Field field) {
        try {
            Attribute attribute = new Attribute();
            attribute.setName(field.name());
            String displayName = field.getProp("displayName");
            if (displayName == null) {
                displayName = field.name();
            }
            attribute.setDisplayName(displayName);
            String type = convertToType(field.schema());
            attribute.setPhysicalDataType(type);
            attribute.setLogicalDataType(field.getProp("logicalType"));
            if (field.getProp("scale") != null) {
                attribute.setScale(Integer.parseInt(field.getProp("scale")));
            }
            if (field.getProp("precision") != null) {
                attribute.setPrecision(Integer.parseInt(field.getProp("precision")));
            }
            if (field.getProp("length") != null) {
                attribute.setLength(Integer.parseInt(field.getProp("length")));
            }
            // standardize date and datetime display
            if (type.equals("date")) {
                attribute.setPropertyValue("dateformat", "YYYY-MM-DD");
            } else if (type.equals("datetime")) {
                attribute.setPropertyValue("dateFormat", "YYYY-MM-DD'T'HH:mm:ss.sssZ");
            }

            if (type.equals("picklist")) {
                String enumValuesString = field.getProp("enumValues");
                List<String> enumValues = Arrays.asList(enumValuesString.split(","));
                attribute.setCleanedUpEnumValues(enumValues);
                attribute.setEnumValues(enumValues);
            }

            return attribute;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read avro field %s", field.name()), e);
        }
    }

    public static String convertToType(Schema schema) {
        String type = null;
        if (schema.getType() == Schema.Type.UNION) {
            // only support [supported-type null] unions.
            if (schema.getTypes().size() != 2) {
            }
            boolean foundNull = false;
            Schema.Type foundType = null;
            for (Schema inner : schema.getTypes()) {
                if (isSupportedAvroType(inner.getType()))
                    foundType = inner.getType();
                if (inner.getType() == Schema.Type.NULL)
                    foundNull = true;
            }
            if (!foundNull || foundType == null) {
                throw new RuntimeException(String.format(
                        "Avro union type must contain a null and a supported type but is %s", schema.getType()));
            }
            type = convertToType(foundType);
        } else {
            type = convertToType(schema.getType());
        }
        return type;
    }

    public static String convertToType(Schema.Type type) {
        if (isSupportedAvroType(type)) {
            return type.toString().toLowerCase();
        }
        throw new RuntimeException(String.format("%s is an unsupported avro type", type));
    }

    public static boolean isSupportedAvroType(Schema.Type type) {
        return supportedAvroTypes.contains(type);
    }
}
