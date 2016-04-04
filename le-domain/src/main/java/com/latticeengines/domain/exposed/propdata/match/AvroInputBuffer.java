package com.latticeengines.domain.exposed.propdata.match;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AvroInputBuffer extends InputBuffer {

    private String avroDir;

    public AvroInputBuffer() {
        setBufferType(Type.AVRO);
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
}
