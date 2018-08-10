package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3Configuration extends ProviderConfiguration {

    @JsonProperty("Bucket")
    private String bucket;

    @JsonIgnore
    private boolean updateCurrentVersion;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public boolean isUpdateCurrentVersion() {
        return updateCurrentVersion;
    }

    public void setUpdateCurrentVersion(boolean updateCurrentVersion) {
        this.updateCurrentVersion = updateCurrentVersion;
    }
}
