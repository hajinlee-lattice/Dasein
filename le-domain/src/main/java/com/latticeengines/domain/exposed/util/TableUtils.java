package com.latticeengines.domain.exposed.util;

import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.lang.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtils {
    public static Table clone(Table source) {
        Table clone = JsonUtils.clone(source);
        clone.setTableType(source.getTableType());
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
            if (attr.getPrecision() != null) {
                fieldBuilder = fieldBuilder.prop("precision", attr.getPrecision().toString());
            }
            if (attr.getScale() != null) {
                fieldBuilder = fieldBuilder.prop("scale", attr.getScale().toString());
            }
            if (attr.getLogicalDataType() != null) {
                fieldBuilder = fieldBuilder.prop("logicalType", attr.getLogicalDataType().toString());
            }
            fieldBuilder = fieldBuilder.prop("sourceLogicalType", attr.getSourceLogicalDataType());
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            for (Map.Entry<String, Object> entry : attr.getEntries()) {
                fieldBuilder.prop(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString());
            }

            if (attr.getEnumValues().size() > 0) {
                fieldBuilder = fieldBuilder.prop("enumValues", StringUtils.join(attr.getEnumValues().toArray(), ","));
            }

            Type type = Type.valueOf(attr.getPhysicalDataType().toUpperCase());

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
                String[] enumValues = new String[attr.getEnumValues().size()];
                attr.getEnumValues().toArray(enumValues);
                fieldAssembler = fieldBuilder.type().enumeration(attr.getName()).symbols(enumValues).noDefault();
                break;
            default:
                break;
            }
        }
        return fieldAssembler.endRecord();
    }
}
