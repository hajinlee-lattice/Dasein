package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnInDerivedPackageRequest {
    private String extensionName;
    private String sourceTableName;

    @JsonProperty("SourceTableName")
    public String getSourceTableName() {
        return sourceTableName;
    }

    @JsonProperty("SourceTableName")
    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    @JsonProperty("ExtensionName")
    public String getExtensionName() {
        return extensionName;
    }

    @JsonProperty("ExtensionName")
    public void setExtensionName(String extensionName) {
        this.extensionName = extensionName;
    }
}
