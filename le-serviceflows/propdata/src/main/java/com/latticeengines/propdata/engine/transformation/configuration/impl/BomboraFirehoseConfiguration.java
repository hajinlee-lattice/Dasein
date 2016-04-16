package com.latticeengines.propdata.engine.transformation.configuration.impl;

import java.util.Map;

import javax.mail.MethodNotSupportedException;

import com.latticeengines.propdata.engine.transformation.configuration.InputSourceConfig;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public class BomboraFirehoseConfiguration implements TransformationConfiguration {
    private String version;
    private String sourceName;
    private Map<String, String> sourceConfigurations;
    private String inputFirehoseVersion;

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public Map<String, String> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @Override
    public InputSourceConfig getInputSourceConfig(String inputSourceName) throws MethodNotSupportedException {
        throw new MethodNotSupportedException();
    }

    @Override
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public void setSourceConfigurations(Map<String, String> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    public String getInputFirehoseVersion() {
        return inputFirehoseVersion;
    }

    public void setInputFirehoseVersion(String inputFirehoseVersion) {
        this.inputFirehoseVersion = inputFirehoseVersion;
    }

}
