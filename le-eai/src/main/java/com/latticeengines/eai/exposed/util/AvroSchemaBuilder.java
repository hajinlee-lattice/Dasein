package com.latticeengines.eai.exposed.util;

import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;

@Component("avroSchemaBuilder")
public class AvroSchemaBuilder {

    public static Schema createSchema(Table table) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(table.getName());
        recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("").fields();
        FieldBuilder<Schema> fieldBuilder;

        for (Attribute attr : table.getAttributes()) {
            fieldBuilder = fieldAssembler.name(attr.getName());

            fieldBuilder = fieldBuilder.prop("displayName", attr.getDisplayName());
            fieldBuilder = fieldBuilder.prop("length", attr.getLength().toString());
            fieldBuilder = fieldBuilder.prop("precision", attr.getPrecision().toString());
            fieldBuilder = fieldBuilder.prop("scale", attr.getScale().toString());
            fieldBuilder = fieldBuilder.prop("logicalType", attr.getLogicalDataType());
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());
            
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
