package com.latticeengines.domain.exposed.spark;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class LocalFileSparkScript extends SparkScript {

    @JsonIgnore
    private File file;

    @Override
    @JsonIgnore
    public Type getType() {
        return Type.LocalFile;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }
}
