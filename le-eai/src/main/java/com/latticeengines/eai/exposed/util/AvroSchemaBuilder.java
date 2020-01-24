package com.latticeengines.eai.exposed.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public final class AvroSchemaBuilder {

    protected AvroSchemaBuilder() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    public static Schema mergeSchemas(Schema schema, Table table) {
        if (table.getAttributes().size() == 0) {
            return schema;
        }
        Map<String, Attribute> attributes = new HashMap<String, Attribute>();

        for (Attribute attr : table.getAttributes()) {
            attributes.put(attr.getName(), attr);
        }
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(schema.getName());

        for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
            recordBuilder.prop(entry.getKey(), entry.getValue());
        }
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("").fields();
        FieldBuilder<Schema> fieldBuilder;
        for (Field field : schema.getFields()) {
            Attribute attr = attributes.get(field.name());
            String displayName = field.getProp("displayName");
            String length = field.getProp("length");
            String precision = field.getProp("precision");
            String scale = field.getProp("scale");
            String logicalType = field.getProp("logicalType");
            String sourceLogicalType = field.getProp("sourceLogicalType");
            String uuid = field.getProp("uuid");
            String enumValues = field.getProp("enumValues");
            Type type = field.schema().getTypes().get(0).getType();

            if (attr != null) {
                if (attr.getDisplayName() != null) {
                    displayName = attr.getDisplayName();
                }

                if (attr.getLength() != null) {
                    length = attr.getLength().toString();
                }
                if (attr.getPrecision() != null) {
                    precision = attr.getPrecision().toString();
                }
                if (attr.getScale() != null) {
                    scale = attr.getScale().toString();
                }

                if (attr.getSourceLogicalDataType() != null) {
                    sourceLogicalType = attr.getSourceLogicalDataType();
                }

                if (attr.getLogicalDataType() != null) {
                    logicalType = attr.getLogicalDataType().toString();
                }

                if (attr.getCleanedUpEnumValues().size() > 0) {
                    enumValues = StringUtils.join(attr.getCleanedUpEnumValues().toArray(), ",");
                }

                assert (attr.getPhysicalDataType() != null);
                type = Type.valueOf(attr.getPhysicalDataType());
            }
            fieldBuilder = fieldAssembler.name(field.name());
            fieldBuilder = fieldBuilder.prop("displayName", displayName);
            fieldBuilder = fieldBuilder.prop("length", length);
            fieldBuilder = fieldBuilder.prop("precision", precision);
            fieldBuilder = fieldBuilder.prop("scale", scale);
            fieldBuilder = fieldBuilder.prop("sourceLogicalType", sourceLogicalType);
            fieldBuilder = fieldBuilder.prop("logicalType", logicalType);
            fieldBuilder = fieldBuilder.prop("uuid", uuid);

            if (enumValues != null) {
                fieldBuilder = fieldBuilder.prop("enumValues", enumValues);
            }

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
            default:
                break;
            }
        }
        return fieldAssembler.endRecord();
    }
}
