package com.latticeengines.domain.exposed.propdata.match;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "BufferType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlInputBuffer.class, name = "SQL"),
        @JsonSubTypes.Type(value = AvroInputBuffer.class, name = "AVRO")
})
public class InputBuffer {

    private Type bufferType;
    private Long numRows;

    @JsonProperty("BufferType")
    public Type getBufferType() {
        return bufferType;
    }

    @JsonProperty("BufferType")
    protected void setBufferType(Type bufferType) {
        this.bufferType = bufferType;
    }

    @JsonIgnore
    public Long getNumRows() {
        return numRows;
    }

    @JsonIgnore
    public void setNumRows(Long numRows) {
        this.numRows = numRows;
    }

    public enum Type {
        SQL, AVRO
    }

}
