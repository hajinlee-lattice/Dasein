package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.joda.time.DateTime;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

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
        return getTable(configuration, path, null, null, false);
    }

    public static Table getTable(Configuration configuration, String path, String primaryKeyName,
            String lastModifiedKeyName) {
        return getTable(configuration, path, primaryKeyName, lastModifiedKeyName, false);
    }

    public static Table getTable(Configuration configuration, String path, String primaryKeyName,
            String lastModifiedKeyName, boolean skipCount) {
        try {
            List<Extract> extracts = convertToExtracts(configuration, path, skipCount);
            Schema schema = AvroUtils.getSchemaFromGlob(configuration, extracts.get(0).getPath());
            return getTable(schema, extracts, primaryKeyName, lastModifiedKeyName, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to parse metadata for avro file located at %s", path), e);
        }
    }

    public static Table getParquetTable(Configuration configuration, String path, String primaryKeyName,
                                 String lastModifiedKeyName, boolean skipCount) {
        try {
            List<Extract> extracts = convertToParquetExtracts(configuration, path, skipCount);
            Schema schema = ParquetUtils.getAvroSchema(configuration, extracts.get(0).getPath());
            return getTable(schema, extracts, primaryKeyName, lastModifiedKeyName, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to parse metadata for parquet file located at %s", path), e);
        }
    }

    public static Table getBucketedTableFromSchemaPath(Configuration configuration, String avroPath,
            String avscPath, String primaryKeyName, String lastModifiedKeyName) {
        try {
            @SuppressWarnings("deprecation")
            Schema schema = Schema.parse(HdfsUtils.getInputStream(configuration, avscPath));
            List<Extract> extracts = convertToExtracts(configuration, avroPath, false);
            Table table = getTable(schema, extracts, primaryKeyName, lastModifiedKeyName, true);
            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "Failed to parse metadata for avro file located at %s, using avsc at %s",
                    avroPath, avscPath), e);
        }
    }

    private static List<Extract> convertToExtracts(Configuration configuration, String avroPath,
            boolean skipCount) throws Exception {
        boolean isDirectory = false;
        if (HdfsUtils.isDirectory(configuration, avroPath)) {
            if (avroPath.endsWith("/")) {
                avroPath = avroPath.substring(0, avroPath.length() - 2);
            }
            avroPath = avroPath + "/*.avro";
            isDirectory = true;
        }
        List<String> matches = HdfsUtils.getFilesByGlob(configuration, avroPath);
        List<Extract> extracts = new ArrayList<>();
        for (String match : matches) {
            Extract extract = new Extract();
            try (FileSystem fs = FileSystem.newInstance(configuration)) {
                extract.setExtractionTimestamp(
                        fs.getFileStatus(new Path(match)).getModificationTime());
            }
            extract.setName("extract");
            if (!skipCount) {
                if (isDirectory) {
                    extract.setProcessedRecords(AvroUtils.count(configuration, avroPath));
                } else {
                    extract.setProcessedRecords(AvroUtils.count(configuration, match));
                }
            }
            extracts.add(extract);
            if (isDirectory) {
                extract.setPath(avroPath);
                break;
            } else {
                extract.setPath(match);
            }
        }
        return extracts;
    }

    private static List<Extract> convertToParquetExtracts(Configuration configuration, String parquetPath,
                                                   boolean skipCount) throws Exception {
        boolean isDirectory = false;
        if (HdfsUtils.isDirectory(configuration, parquetPath)) {
            if (parquetPath.endsWith("/")) {
                parquetPath = parquetPath.substring(0, parquetPath.length() - 2);
            }
            parquetPath = parquetPath + "/*.parquet";
            isDirectory = true;
        }
        List<String> matches = HdfsUtils.getFilesByGlob(configuration, parquetPath);
        List<Extract> extracts = new ArrayList<>();
        for (String match : matches) {
            Extract extract = new Extract();
            try (FileSystem fs = FileSystem.newInstance(configuration)) {
                extract.setExtractionTimestamp(
                        fs.getFileStatus(new Path(match)).getModificationTime());
            }
            extract.setName("extract");
            if (!skipCount) {
                if (isDirectory) {
                    extract.setProcessedRecords(ParquetUtils.countParquetFiles(configuration, parquetPath));
                } else {
                    extract.setProcessedRecords(ParquetUtils.countParquetFiles(configuration, match));
                }
            }
            extracts.add(extract);
            if (isDirectory) {
                extract.setPath(parquetPath);
                break;
            } else {
                extract.setPath(match);
            }
        }
        return extracts;
    }

    public static Table getTable(Schema schema, List<Extract> extracts, String primaryKeyName,
            String lastModifiedKeyName, boolean isBucketed) {
        try {
            Table table = new Table();
            table.setName(schema.getName());
            table.setDisplayName(schema.getFullName());

            List<Schema.Field> fields = schema.getFields();

            if (!isBucketed) {
                fields.forEach(field -> table.addAttribute(getAttribute(field)));
            } else {
                fields.forEach(field -> table.addAttributes(getBucketedAttributes(field)));
            }

            table.setExtracts(extracts);

            if (primaryKeyName != null) {
                Attribute primaryKeyAttr = table.getAttribute(primaryKeyName);
                if (primaryKeyAttr == null) {
                    throw new RuntimeException(
                            String.format("Could not locate primary key field %s in avro schema",
                                    primaryKeyName));
                }
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.setName(primaryKeyName);
                primaryKey.addAttribute(primaryKeyName);
                primaryKey.setDisplayName(primaryKeyName);
                table.setPrimaryKey(primaryKey);
            }

            if (lastModifiedKeyName != null) {
                Attribute lastModifiedAttr = table.getAttribute(lastModifiedKeyName);
                if (lastModifiedAttr == null) {
                    throw new RuntimeException(String.format(
                            "Could not locate last modified key field %s in avro schema",
                            lastModifiedKeyName));
                }

                LastModifiedKey lastModifiedKey = new LastModifiedKey();
                lastModifiedKey.setName(lastModifiedKeyName);
                lastModifiedKey.addAttribute(lastModifiedKeyName);
                lastModifiedKey.setDisplayName(lastModifiedKeyName);
                lastModifiedKey.setLastModifiedTimestamp(DateTime.now().getMillis());
                table.setLastModifiedKey(lastModifiedKey);
            }
            table.setTableType(TableType.DATATABLE);
            return table;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to read avro schema from %s", schema.getName()), e);
        }
    }

    public static Schema getAvroSchema(Configuration configuration, Table table) {

        if (table.getExtracts().size() == 0) {
            throw new RuntimeException(
                    String.format("Table %s does not have any extracts", table.getName()));
        }

        return getAvroSchema(configuration, table.getExtracts().get(0));
    }

    public static Schema getAvroSchema(Configuration configuration, Extract extract) {
        return AvroUtils.getSchema(configuration, new Path(extract.getPath()));
    }

    @SuppressWarnings("deprecation")
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
            if (field.getProp("Nullable") != null) {
                attribute.setNullable(Boolean.valueOf(field.getProp("Nullable")));
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
            }
            if (StringUtils.isNotBlank(field.getProp("groups"))) {
                List<ColumnSelection.Predefined> groups = Arrays
                        .stream(field.getProp("groups").split(","))
                        .map(ColumnSelection.Predefined::fromName).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(groups)) {
                    attribute.setGroupsViaList(groups);
                }
            }
            AttributeUtils.setPropertiesFromStrings(attribute, field.props());

            return attribute;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read avro field %s", field.name()),
                    e);
        }
    }

    public static List<Attribute> getBucketedAttributes(Schema.Field field) {
        List<Attribute> attrs = new ArrayList<>();
        String fieldName = field.name();
        JsonNode bucketedAttrs = field.getJsonProp("bucketed_attrs");

        List<ColumnSelection.Predefined> groups = new ArrayList<>();
        if (StringUtils.isNotBlank(field.getProp("groups"))) {
            groups = Arrays.stream(field.getProp("groups").split(","))
                    .map(ColumnSelection.Predefined::fromName).collect(Collectors.toList());
        }

        if (bucketedAttrs == null) {
            attrs.add(getAttribute(field));
        } else {
            for (JsonNode bucketedAttr : (ArrayNode) bucketedAttrs) {
                Attribute attr = new Attribute();
                attr.setName(bucketedAttr.get("nominal_attr").asText());
                attr.setDisplayName(attr.getName());
                attr.setPhysicalDataType(Schema.Type.STRING.getName());
                attr.setBitOffset(bucketedAttr.get("lowest_bit").asInt());
                attr.setNumOfBits(bucketedAttr.get("num_bits").asInt());
                attr.setPhysicalName(fieldName);
                attr.setNullable(Boolean.TRUE);
                if (CollectionUtils.isNotEmpty(groups)) {
                    attr.setGroupsViaList(groups);
                }
                attrs.add(attr);
            }
        }
        return attrs;
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
                        "Avro union type must contain a null and a supported type but is %s",
                        schema.getType()));
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
