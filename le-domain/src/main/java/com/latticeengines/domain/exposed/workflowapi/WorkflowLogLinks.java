package com.latticeengines.domain.exposed.workflowapi;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class WorkflowLogLinks {

    @JsonProperty("AppMasterURL")
    private String appMasterUrl;

    @JsonProperty("S3LogDir")
    private String s3LogDir;

    public String getAppMasterUrl() {
        return appMasterUrl;
    }

    public void setAppMasterUrl(String appMasterUrl) {
        this.appMasterUrl = appMasterUrl;
    }

    public String getS3LogDir() {
        return s3LogDir;
    }

    public void setS3LogDir(String s3LogDir) {
        this.s3LogDir = s3LogDir;
    }
}
