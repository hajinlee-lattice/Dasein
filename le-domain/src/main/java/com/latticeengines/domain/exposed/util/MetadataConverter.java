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
import org.joda.time.DateTime;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
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

    public static Table getTable(Configuration configuration, String path) {
        return getTable(configuration, path, null, null);
    }

    public static Table getTable(Configuration configuration, String path, String primaryKeyName,
            String lastModifiedKeyName) {
        try {
            boolean isDirectory = false;
            if (HdfsUtils.isDirectory(configuration, path)) {
                if (path.endsWith("/")) {
                    path = path.substring(0, path.length() - 2);
                }
                path = path + "/*.avro";
                isDirectory = true;
            }
            List<String> matches = HdfsUtils.getFilesByGlob(configuration, path);

            Schema schema = AvroUtils.getSchemaFromGlob(configuration, path);
            List<Extract> extracts = new ArrayList<Extract>();
            for (String match : matches) {
                Extract extract = new Extract();
                try (FileSystem fs = FileSystem.newInstance(configuration)) {
                    extract.setExtractionTimestamp(fs.getFileStatus(new Path(match)).getModificationTime());
                }
                extract.setName("extract");
                extracts.add(extract);
                if (isDirectory) {
                    extract.setPath(path);
                    break;
                } else {
                    extract.setPath(match);
                }
            }

            Table table = getTable(schema, extracts, primaryKeyName, lastModifiedKeyName);
            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse metadata for avro file located at %s", path), e);
        }
    }

    public static Table getTable(Schema schema, List<Extract> extracts, String primaryKeyName,
            String lastModifiedKeyName) {
        try {
            Table table = new Table();
            table.setName(schema.getName());
            table.setDisplayName(schema.getFullName());

            List<Schema.Field> fields = schema.getFields();

            for (Schema.Field field : fields) {
                Attribute attr = getAttribute(field);
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
                primaryKey.setDisplayName(primaryKeyName);
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
                lastModifiedKey.setDisplayName(lastModifiedKeyName);
                lastModifiedKey.setLastModifiedTimestamp(DateTime.now().getMillis());
                table.setLastModifiedKey(lastModifiedKey);
            }

            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read avro schema from %s", schema.getName()), e);
        }
    }

    public static Schema getAvroSchema(Configuration configuration, Table table) {

        if (table.getExtracts().size() == 0) {
            throw new RuntimeException(String.format("Table %s does not have any extracts", table.getName()));
        }

        return getAvroSchema(configuration, table.getExtracts().get(0));
    }

    public static Schema getAvroSchema(Configuration configuration, Extract extract) {
        return AvroUtils.getSchema(configuration, new Path(extract.getPath()));
    }

    public static Attribute getAttribute(Schema.Field field) {
        try {
            Attribute attribute = new Attribute();
            attribute.setName(field.name());
            String displayName = field.getProp("displayName");
            if (displayName == null) {
                displayName = field.name();
            }
            attribute.setDisplayName(displayName);
            String type = getType(field.schema());
            attribute.setPhysicalDataType(type);
            attribute.setSourceLogicalDataType(field.getProp("sourceLogicalType"));
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
            attribute.setInterfaceName(field.getProp("InterfaceName"));

            return attribute;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read avro field %s", field.name()), e);
        }
    }

    public static String getType(Schema schema) {
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
            type = getType(foundType);
        } else {
            type = getType(schema.getType());
        }
        return type;
    }

    public static String getType(Schema.Type type) {
        if (isSupportedAvroType(type)) {
            return type.toString().toLowerCase();
        }
        throw new RuntimeException(String.format("%s is an unsupported avro type", type));
    }

    public static boolean isSupportedAvroType(Schema.Type type) {
        return supportedAvroTypes.contains(type);
    }
}
