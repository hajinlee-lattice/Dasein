package com.latticeengines.domain.exposed.util;

import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtils {
    public static Table clone(Table source, String name) {
        Table clone = JsonUtils.clone(source);
        clone.setTableType(source.getTableType());
        clone.setName(name);
        return clone;
    }

    public static Schema createSchema(String name, Table table) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(name);
        recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("").fields();
        FieldBuilder<Schema> fieldBuilder;

        for (Attribute attr : table.getAttributes()) {
            fieldBuilder = fieldAssembler.name(attr.getName());

            fieldBuilder = fieldBuilder.prop("displayName", attr.getDisplayName());

            if (attr.getLength() != null) {
                fieldBuilder = fieldBuilder.prop("length", attr.getLength().toString());
            }
            if (CollectionUtils.isNotEmpty(attr.getGroupsAsList())) {
                String str = StringUtils.join(attr.getGroupsAsList(), ",");
                fieldBuilder = fieldBuilder.prop("groups", str);
            }
            if (attr.getPrecision() != null) {
                fieldBuilder = fieldBuilder.prop("precision", attr.getPrecision().toString());
            }
            if (attr.getScale() != null) {
                fieldBuilder = fieldBuilder.prop("scale", attr.getScale().toString());
            }
            if (attr.getLogicalDataType() != null) {
                fieldBuilder = fieldBuilder.prop("logicalType",
                        attr.getLogicalDataType().toString());
            }
            if (attr.getNullable() != null) {
                fieldBuilder = fieldBuilder.prop("Nullable", attr.getNullable().toString());
            }
            if (attr.getSourceLogicalDataType() != null) {
                fieldBuilder = fieldBuilder.prop("sourceLogicalType",
                        attr.getSourceLogicalDataType());
            }
            String groups = CollectionUtils.isNotEmpty(attr.getGroupsAsList())
                    ? StringUtils.join(attr.getGroupsAsList(), ",") : null;
            if (StringUtils.isNotBlank(groups)) {
                fieldBuilder = fieldBuilder.prop("groups", groups);
            }
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            for (Map.Entry<String, Object> entry : attr.getEntries()) {
                fieldBuilder.prop(entry.getKey(),
                        entry.getValue() == null ? "" : entry.getValue().toString());
            }

            if (attr.getCleanedUpEnumValues().size() > 0) {
                fieldBuilder = fieldBuilder.prop("enumValues",
                        attr.getCleanedUpEnumValuesAsString());
            }

            Type type = getTypeFromPhysicalDataType(attr.getPhysicalDataType().toUpperCase());

            switch (type) {
                case DOUBLE:
                    fieldAssembler = fieldBuilder.type().unionOf().doubleType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case FLOAT:
                    fieldAssembler = fieldBuilder.type().unionOf().floatType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case INT:
                    fieldAssembler = fieldBuilder.type().unionOf().intType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case LONG:
                    fieldAssembler = fieldBuilder.type().unionOf().longType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case STRING:
                    fieldAssembler = fieldBuilder.type().unionOf().stringType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case BOOLEAN:
                    fieldAssembler = fieldBuilder.type().unionOf().booleanType().and().nullType()
                            .endUnion().noDefault();
                    break;
                case ENUM:
                    String[] enumValues = new String[attr.getCleanedUpEnumValues().size()];
                    attr.getCleanedUpEnumValues().toArray(enumValues);
                    fieldAssembler = fieldBuilder.type().enumeration(attr.getName())
                            .symbols(enumValues).noDefault();
                    break;
                default:
                    break;
            }
        }
        return fieldAssembler.endRecord();
    }

    public static Type getTypeFromPhysicalDataType(String dataType) {
        if (StringUtils.isEmpty(dataType)) {
            throw new IllegalArgumentException("Physical data type cannot be null!");
        }
        dataType = dataType.toUpperCase();
        if (dataType.startsWith("VARCHAR") || dataType.startsWith("NVARCHAR")) {
            return Type.STRING;
        }
        switch (dataType) {
            case "BIT":
                return Type.BOOLEAN;
            case "BYTE":
            case "SHORT":
                return Type.INT;
            case "DATE":
            case "DATETIME":
            case "DATETIMEOFFSET":
                return Type.LONG;
            default:
                return Type.valueOf(dataType);
        }
    }

    public static String getFullTableName(String tableNamePrefix, String version) {
        if (StringUtils.isNotBlank(version)) {
            return tableNamePrefix + "_" + version;
        } else {
            return tableNamePrefix;
        }
    }

    public static String getAvscGlob(String avroPath) throws IllegalArgumentException {
        String pattern = "/Pods/.*/Contracts/.*/Tenants/.*/Spaces/Production/Data/Tables/.*";
        if (avroPath.matches(pattern)) {
            String avscPath = avroPath.replace("/Tables/", "/TableSchemas/");
            if (avscPath.endsWith(".avro")) {
                avscPath = avscPath.substring(0, avscPath.lastIndexOf("/"));
            }
            if (!avscPath.endsWith("/")) {
                avscPath += "/";
            }
            return avscPath + "*.avsc";
        } else {
            throw new IllegalArgumentException(
                    "The avro path does not match regex pattern " + pattern);
        }
    }

}
