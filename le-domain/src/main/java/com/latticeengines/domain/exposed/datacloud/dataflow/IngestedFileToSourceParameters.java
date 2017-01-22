package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestedFileToSourceParameters extends TransformationFlowParameters {
    @JsonProperty("IngetionName")
    private String ingetionName;

    @JsonProperty("Qualifier")
    private String qualifier = "\"";

    @JsonProperty("Delimiter")
    private String delimiter = ",";

    @JsonProperty("Extension")
    private String extension = ".csv";

    @JsonProperty("Charset")
    private String charset;

    @JsonProperty("FileName")
    private String fileName;

    public String getIngetionName() {
        return ingetionName;
    }

    public void setIngetionName(String ingetionName) {
        this.ingetionName = ingetionName;
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

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
