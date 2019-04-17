package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.IngestedFileToSourceTransformerConfig.CompressType;

public class IngestedFileToSourceParameters extends TransformationFlowParameters {
    @JsonProperty("IngestionName")
    private String ingestionName;

    @JsonProperty("Qualifier")
    private String qualifier = "\"";

    @JsonProperty("Delimiter")
    private String delimiter = ",";

    @JsonProperty("Charset")
    private String charset;

    @JsonProperty("FileNameOrExtension")
    private String fileNameOrExtension;

    @JsonProperty("CompressedFileNameOrExtension")
    private String compressedFileNameOrExtension;

    @JsonProperty("CompressType")
    private CompressType compressType;

    @JsonIgnore
    private String applicationId;

    public String getIngestionName() {
        return ingestionName;
    }

    public void setIngestionName(String ingestionName) {
        this.ingestionName = ingestionName;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getFileNameOrExtension() {
        return fileNameOrExtension;
    }

    public void setFileNameOrExtension(String fileNameOrExtension) {
        this.fileNameOrExtension = fileNameOrExtension;
    }

    public String getCompressedFileNameOrExtension() {
        return compressedFileNameOrExtension;
    }

    public void setCompressedFileNameOrExtension(String compressedFileNameOrExtension) {
        this.compressedFileNameOrExtension = compressedFileNameOrExtension;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

}
