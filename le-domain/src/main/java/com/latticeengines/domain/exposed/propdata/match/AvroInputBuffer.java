package com.latticeengines.domain.exposed.propdata.match;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;

public class AvroInputBuffer extends InputBuffer {

    private String avroDir;
    private Schema schema;

    public AvroInputBuffer() {
        setBufferType(IOBufferType.AVRO);
    }

    @JsonProperty("AvroDir")
    public String getAvroDir() {
        return avroDir;
    }

    @JsonProperty("AvroDir")
    public void setAvroDir(String avroDir) {
        if (avroDir.endsWith("/*.avro") || avroDir.endsWith("/")) {
            avroDir = avroDir.substring(0, avroDir.lastIndexOf("/"));
        }
        this.avroDir = avroDir;
    }

    @JsonProperty("Schema")
    private JsonNode getSchemaAsJson() {
        try {
            if (schema != null) {
                return new ObjectMapper().readTree(schema.toString());
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse avro schema to json node", e);
        }
    }

    @JsonProperty("Schema")
    private void setSchemaAsJson(JsonNode schema) {
        if (schema != null && StringUtils.isNotEmpty(schema.toString())
                && !"null".equalsIgnoreCase(schema.toString())) {
            this.schema = new Schema.Parser().parse(schema.toString());
        } else {
            this.schema = null;
        }
    }

    @JsonIgnore
    public Schema getSchema() {
        return schema;
    }

    @JsonIgnore
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
