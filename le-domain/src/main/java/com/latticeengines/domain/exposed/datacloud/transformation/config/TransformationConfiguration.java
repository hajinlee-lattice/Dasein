package com.latticeengines.domain.exposed.datacloud.transformation.config;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

public interface TransformationConfiguration {

    String getSourceName();

    void setSourceName(String sourceName);

    Map<String, String> getSourceConfigurations();

    void setSourceConfigurations(Map<String, String> sourceConfigurations);

    InputSourceConfig getInputSourceConfig() throws UnsupportedOperationException;

    String getVersion();

    void setVersion(String newLatestVersion);

    List<String> getBaseVersions();

    void setBaseVersions(List<String> baseVersions);

    String getServiceBeanName();

    void setServiceBeanName(String serviceBeanName);

    String getRootOperationId();

    void setRootOperationId(String rootOperationId);

    List<SourceColumn> getSourceColumns();

    void setSourceColumns(List<SourceColumn> sourceColumns);
}
