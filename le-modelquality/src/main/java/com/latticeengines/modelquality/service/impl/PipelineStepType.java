package com.latticeengines.modelquality.service.impl;

public enum PipelineStepType {

    PYTHONLEARNING("py"), //
    PYTHONRTS("py"), //
    METADATA("json");

    private String extension;

    PipelineStepType(String extension) {
        this.extension = extension;
    }

    public String getFileExtension() {
        return extension;
    }
}
