package com.latticeengines.domain.exposed.metadata.statistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubcategoryStatistics {

    @JsonDeserialize(keyUsing = ColumnLookupKeyDeserializer.class)
    @JsonSerialize(keyUsing = ColumnLookupKeySerializer.class)
    private Map<ColumnLookup, AttributeStatistics> attributes = new HashMap<>();

    public Map<ColumnLookup, AttributeStatistics> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<ColumnLookup, AttributeStatistics> attributes) {
        this.attributes = attributes;
    }

    private static class ColumnLookupKeyDeserializer extends KeyDeserializer {
        public ColumnLookupKeyDeserializer() {
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
            String[] elements = key.split("\\.");
            if (elements.length == 1) {
                return new ColumnLookup(elements[0]);
            } else if (elements.length != 2) {
                throw new RuntimeException(String.format("Cannot deserialize: %s", key));
            } else {
                return new ColumnLookup(SchemaInterpretation.valueOf(elements[0]), elements[1]);
            }
        }
    }

    private static class ColumnLookupKeySerializer extends JsonSerializer<ColumnLookup> {
        public ColumnLookupKeySerializer() {
        }

        @Override
        public void serialize(ColumnLookup value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            String str = value.getColumnName();
            if (value.getObjectType() != null) {
                str = value.getObjectType() + "." + str;
            }
            jgen.writeFieldName(str);
        }
    }
}
