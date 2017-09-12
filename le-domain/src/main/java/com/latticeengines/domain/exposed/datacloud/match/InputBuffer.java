package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "BufferType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlInputBuffer.class, name = "SQL"),
        @JsonSubTypes.Type(value = AvroInputBuffer.class, name = "AVRO")
})
public class InputBuffer {

    private IOBufferType bufferType;

    @JsonProperty("BufferType")
    public IOBufferType getBufferType() {
        return bufferType;
    }

    @JsonProperty("BufferType")
    protected void setBufferType(IOBufferType bufferType) {
        this.bufferType = bufferType;
    }

}
