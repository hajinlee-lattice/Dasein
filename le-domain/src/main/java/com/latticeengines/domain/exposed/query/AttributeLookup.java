package com.latticeengines.domain.exposed.query;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;


@JsonSerialize(using = AttributeLookup.AttributeLookupSerializer.class)
@JsonDeserialize(using = AttributeLookup.AttributeLookupDeserializer.class)
public class AttributeLookup extends Lookup {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("attribute")
    private String attribute;

    public AttributeLookup() {
    }

    public AttributeLookup(BusinessEntity entity, String attrName) {
        this.entity = entity;
        this.attribute = attrName;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof AttributeLookup)) {
            return false;
        }
        return this.toString().equals(obj.toString());
    }

    @Override
    public String toString() {
        return entity.name() + "." + attribute;
    }

    public static AttributeLookup fromString(String str) {
        if (StringUtils.isNotBlank(str)) {
            try {
                String[] tokens = str.split("\\.");
                BusinessEntity entity = BusinessEntity.valueOf(tokens[0]);
                String attrName = str.replace(tokens[0] + ".", "");
                return new AttributeLookup(entity, attrName);
            } catch (Exception e) {
                throw new RuntimeException("Cannot parse [" + str + "] to AttributeLookup");
            }
        } else {
            return null;
        }
    }

    public static class AttributeLookupDeserializer extends JsonDeserializer<AttributeLookup> {
        public AttributeLookupDeserializer() {
        }

        @Override
        public AttributeLookup deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            ObjectCodec oc = jp.getCodec();
            JsonNode node = oc.readTree(jp);
            String str = node.asText();
            return AttributeLookup.fromString(str);
        }
    }

    public static class AttributeLookupSerializer extends JsonSerializer<AttributeLookup> {
        public AttributeLookupSerializer() {
        }

        @Override
        public void serialize(AttributeLookup value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeFieldName(value.toString());
        }

        @Override
        public void serializeWithType(AttributeLookup value, JsonGenerator gen, SerializerProvider provider,
                TypeSerializer typeSer) throws IOException {
            typeSer.writeTypePrefixForObject(value, gen);
            serialize(value, gen, provider); // call your customized serialize
                                             // method
            typeSer.writeTypeSuffixForObject(value, gen);
        }
    }

    public static class AttributeLookupKeyDeserializer extends KeyDeserializer {
        public AttributeLookupKeyDeserializer() {
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
            String[] elements = key.split("\\.");
            if (elements.length < 2) {
                throw new RuntimeException(String.format("Cannot deserialize: %s", key));
            } else {
                return new AttributeLookup(BusinessEntity.valueOf(elements[0]), key.replace(elements[0] + ".", ""));
            }
        }
    }

}
