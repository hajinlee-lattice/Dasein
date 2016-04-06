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

    private IOBufferType bufferType;
    private Long numRows;

    @JsonProperty("BufferType")
    public IOBufferType getBufferType() {
        return bufferType;
    }

    @JsonProperty("BufferType")
    protected void setBufferType(IOBufferType bufferType) {
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

}
