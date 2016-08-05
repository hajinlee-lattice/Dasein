package com.latticeengines.propdata.engine.transformation.configuration;

import org.springframework.stereotype.Component;

@Component("fileInputSourceConfig")
public abstract class FileInputSourceConfig implements InputSourceConfig {
    protected String version;

    protected String qualifier;

    protected String delimiter;

    protected String extension;

    protected String charset;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public abstract String getQualifier();

    public abstract String getDelimiter();

    public abstract String getExtension();

    public abstract String getCharset();
}
