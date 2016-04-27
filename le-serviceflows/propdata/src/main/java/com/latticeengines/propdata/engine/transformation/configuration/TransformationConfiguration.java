package com.latticeengines.propdata.engine.transformation.configuration;

import java.util.Map;

import javax.mail.MethodNotSupportedException;

public interface TransformationConfiguration {
    String getSourceName();

    Map<String, String> getSourceConfigurations();

    InputSourceConfig getInputSourceConfig(String inputSourceName) throws MethodNotSupportedException;

    String getVersion();

    String getServiceBeanName();

    String getRootOperationId();

    void setRootOperationId(String rootOperationId);
}
