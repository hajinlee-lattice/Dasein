package com.latticeengines.domain.exposed.spark;

import java.io.InputStream;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class InputStreamSparkScript extends SparkScript {

    @JsonIgnore
    private InputStream stream;

    @Override
    @JsonIgnore
    public Type getType() {
        return Type.InputStream;
    }

    public InputStream getStream() {
        return stream;
    }

    public void setStream(InputStream stream) {
        this.stream = stream;
    }
}
