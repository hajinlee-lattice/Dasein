package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportSourceDataConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({ @Type(value = ImportSourceDataConfiguration.class, name = "ImportSourceDataConfiguration"), })
public class ImportStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    @JsonProperty("source_type")
    private SourceType sourceType;

    @JsonProperty("source_file_name")
    private String sourceFileName;

    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }

}
