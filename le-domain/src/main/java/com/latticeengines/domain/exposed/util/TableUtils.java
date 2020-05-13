package com.latticeengines.domain.exposed.util;

import java.util.HashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public final class TableUtils {

    private static final Logger log = LoggerFactory.getLogger(TableUtils.class);
    protected TableUtils() {
        throw new UnsupportedOperationException();
    }
    public static Table clone(Table source, String name) {
        return clone(source, name, false);
    }

    public static Table clone(Table source, String name, boolean ignoreExtracts) {
        Table clone = JsonUtils.clone(source);
        clone.setTableType(source.getTableType());
        clone.setName(name);
        clone.setExtracts(ignoreExtracts ? null : clone.getExtracts());
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
                fieldBuilder = fieldBuilder.prop("logicalType", attr.getLogicalDataType().toString());
            }
            if (attr.getNullable() != null) {
                fieldBuilder = fieldBuilder.prop("Nullable", attr.getNullable().toString());
            }
            if (attr.getSourceLogicalDataType() != null) {
                fieldBuilder = fieldBuilder.prop("sourceLogicalType", attr.getSourceLogicalDataType());
            }
            String groups = CollectionUtils.isNotEmpty(attr.getGroupsAsList())
                    ? StringUtils.join(attr.getGroupsAsList(), ",")
                    : null;
            if (StringUtils.isNotBlank(groups)) {
                fieldBuilder = fieldBuilder.prop("groups", groups);
            }
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            for (Map.Entry<String, Object> entry : attr.getEntries()) {
                fieldBuilder.prop(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString());
            }

            if (attr.getCleanedUpEnumValues().size() > 0) {
                fieldBuilder = fieldBuilder.prop("enumValues", attr.getCleanedUpEnumValuesAsString());
            }

            Type type = getTypeFromPhysicalDataType(attr.getPhysicalDataType().toUpperCase());

            switch (type) {
            case DOUBLE:
                fieldAssembler = fieldBuilder.type().unionOf().doubleType().and().nullType().endUnion().noDefault();
                break;
            case FLOAT:
                fieldAssembler = fieldBuilder.type().unionOf().floatType().and().nullType().endUnion().noDefault();
                break;
            case INT:
                fieldAssembler = fieldBuilder.type().unionOf().intType().and().nullType().endUnion().noDefault();
                break;
            case LONG:
                fieldAssembler = fieldBuilder.type().unionOf().longType().and().nullType().endUnion().noDefault();
                break;
            case STRING:
                fieldAssembler = fieldBuilder.type().unionOf().stringType().and().nullType().endUnion().noDefault();
                break;
            case BOOLEAN:
                fieldAssembler = fieldBuilder.type().unionOf().booleanType().and().nullType().endUnion().noDefault();
                break;
            case ENUM:
                String[] enumValues = new String[attr.getCleanedUpEnumValues().size()];
                attr.getCleanedUpEnumValues().toArray(enumValues);
                fieldAssembler = fieldBuilder.type().enumeration(attr.getName()).symbols(enumValues).noDefault();
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
            throw new IllegalArgumentException("The avro path does not match regex pattern " + pattern);
        }
    }

    // TODO(jwinter): Make sure all necessary fields are being compared.  Make sure it is ok to skip modeling fields.
    // Compare two tables and return true if they are identical and false otherwise.
    public static boolean compareMetadataTables(Table table1, Table table2) {
        if ((table1 == null || table2 == null) && table1 != table2) {
            return false;
        }
        if (!StringUtils.equals(table1.getName(), table2.getName())) {
            return false;
        }
        if (table1.getAttributes().size() != table2.getAttributes().size()) {
            return false;
        }
        HashMap<String, Attribute> table2Attrs = new HashMap<>();
        for (Attribute attr2 : table2.getAttributes()) {
            table2Attrs.put(attr2.getName(), attr2);
        }


        for (Attribute attr1 : table1.getAttributes()) {
            if (!table2Attrs.containsKey(attr1.getName())) {
                return false;
            }
            Attribute attr2 = table2Attrs.get(attr1.getName());
            if (!StringUtils.equals(attr1.getDisplayName(), attr2.getDisplayName())) {
                return false;
            }
            if (!StringUtils.equalsIgnoreCase(attr1.getPhysicalDataType(), attr2.getPhysicalDataType())) {
                return false;
            }
            // TODO(jwinter): Do we need to check required?
            if (attr1.getRequired() != attr2.getRequired()) {
                return false;
            }
            if (!StringUtils.equals(attr1.getDateFormatString(), attr2.getDateFormatString())) {
                return false;
            }
            if (!StringUtils.equals(attr1.getTimeFormatString(), attr2.getTimeFormatString())) {
                return false;
            }
            if (!StringUtils.equals(attr1.getTimezone(), attr2.getTimezone())) {
                return false;
            }
        }
        return true;
    }

    // Merge new metadata table into existing table.
    // TODO(jwinter): It is concerning that modeling fields are not copied (eg. approvedUsage, category,
    // logicalDataType).
    public static Table mergeMetadataTables(Table existingTable, Table newTable) {
        log.info("Merging table {} into table {}", newTable.getName(), existingTable.getName());

        HashMap<String, Attribute> existingAttrMap = new HashMap<>();
        for (Attribute attr : existingTable.getAttributes()) {
            existingAttrMap.put(attr.getName(), attr);
        }

        for (Attribute newAttr : newTable.getAttributes()) {
            if (!existingAttrMap.containsKey(newAttr.getName())) {
                log.info("Copying over new attribute {}", newAttr.getName());
                Attribute copyAttr = new Attribute(newAttr.getName());
                // TODO(jwinter): Does this copy both field and properties?
                AttributeUtils.copyPropertiesFromAttribute(newAttr, copyAttr);
                existingTable.addAttribute(newAttr);
            } else {
                log.info("Copying over existing attribute {}", newAttr.getName());
                // TODO(jwinter): Do we not have to copy more fields?
                // TODO(jwinter): What about the property bag?
                Attribute existingAttr = existingAttrMap.get(newAttr.getName());
                existingAttr.setDisplayName(newAttr.getDisplayName());
                // TODO(jwinter): I believe we need physicalDataType?
                existingAttr.setRequired(newAttr.getRequired());
                existingAttr.setPhysicalDataType(newAttr.getPhysicalDataType());
                existingAttr.setDateFormatString(newAttr.getDateFormatString());
                existingAttr.setTimeFormatString(newAttr.getTimeFormatString());
                existingAttr.setTimezone(newAttr.getTimezone());
                // TODO(jwinter): This is likely only for VisiDB import.
                if (newAttr.getSourceAttrName() != null) {
                    existingAttr.setSourceAttrName(newAttr.getSourceAttrName());
                }
            }
        }
        return existingTable;
    }
}
