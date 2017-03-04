package com.latticeengines.domain.exposed.datafabric.generic;

public class GenericFabricStatus {

    private GenericFabricStatusEnum status;
    private float progress;
    private String message;

    public GenericFabricStatusEnum getStatus() {
        return status;
    }

    public void setStatus(GenericFabricStatusEnum status) {
        this.status = status;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
