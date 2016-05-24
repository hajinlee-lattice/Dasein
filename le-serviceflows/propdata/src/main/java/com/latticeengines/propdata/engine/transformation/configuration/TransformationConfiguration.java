package com.latticeengines.propdata.engine.transformation.configuration;

import java.util.List;
import java.util.Map;

import javax.mail.MethodNotSupportedException;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

public interface TransformationConfiguration {
    String getSourceName();

    Map<String, String> getSourceConfigurations();

    InputSourceConfig getInputSourceConfig(String inputSourceName) throws MethodNotSupportedException;

    String getVersion();

    String getServiceBeanName();

    String getRootOperationId();

    void setRootOperationId(String rootOperationId);

    List<SourceColumn> getSourceColumns();

    void setSourceName(String sourceName);

    void setSourceConfigurations(Map<String, String> sourceConfigurations);

    void setVersion(String newLatestVersion);

    void setSourceColumns(List<SourceColumn> sourceColumns);
}
