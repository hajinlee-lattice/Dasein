package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CopySourceFileRequest {

    @JsonProperty("OriginalSourceFile")
    private String originalSourceFile;

    @JsonProperty("TargetTable")
    private String targetTable;

    @JsonProperty("TargetTenant")
    private String targetTenant;

    public String getOriginalSourceFile() {
        return originalSourceFile;
    }

    public void setOriginalSourceFile(String originalSourceFile) {
        this.originalSourceFile = originalSourceFile;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getTargetTenant() {
        return targetTenant;
    }

    public void setTargetTenant(String targetTenant) {
        this.targetTenant = targetTenant;
    }
}
