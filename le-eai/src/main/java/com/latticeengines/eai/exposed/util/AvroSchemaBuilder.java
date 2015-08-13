package com.latticeengines.eai.exposed.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeOwner;

public class AvroSchemaBuilder {
    
    @SuppressWarnings("deprecation")
    public static Schema mergeSchemas(Schema schema, AttributeOwner attributeOwner) {
        if (attributeOwner.getAttributes().size() == 0) {
            return schema;
        }
        Map<String, Attribute> attributes = new HashMap<String, Attribute>();
        
        for (Attribute attr : attributeOwner.getAttributes()) {
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
                
                if (attr.getLogicalDataType() != null) {
                    logicalType = attr.getLogicalDataType();
                }
                
                if (attr.getEnumValues().size() > 0) {
                    enumValues = StringUtils.join(attr.getEnumValues().toArray(), ",");
                }
                
                assert(attr.getPhysicalDataType() != null);
                type = Type.valueOf(attr.getPhysicalDataType());
            }
            fieldBuilder = fieldAssembler.name(field.name());
            fieldBuilder = fieldBuilder.prop("displayName", displayName);
            fieldBuilder = fieldBuilder.prop("length", length);
            fieldBuilder = fieldBuilder.prop("precision", precision);
            fieldBuilder = fieldBuilder.prop("scale", scale);
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

    public static Schema createSchema(String name, AttributeOwner attributeOwner) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(name);
        recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("").fields();
        FieldBuilder<Schema> fieldBuilder;

        for (Attribute attr : attributeOwner.getAttributes()) {
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
            fieldBuilder = fieldBuilder.prop("logicalType", attr.getLogicalDataType());
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());
            
            for (Map.Entry<String, Object> entry : attr.getEntries()) {
                fieldBuilder.prop(entry.getKey(), entry.getValue().toString());
            }
            
            if (attr.getEnumValues().size() > 0) {
                fieldBuilder = fieldBuilder.prop("enumValues", StringUtils.join(attr.getEnumValues().toArray(), ","));
            }

            Type type = Type.valueOf(attr.getPhysicalDataType());

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
