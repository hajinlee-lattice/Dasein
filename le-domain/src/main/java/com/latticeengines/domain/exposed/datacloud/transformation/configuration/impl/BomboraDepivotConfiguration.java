package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.InputSourceConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;

public class BomboraDepivotConfiguration extends BasicTransformationConfiguration
        implements TransformationConfiguration {
    private String sourceName;
    private String version;
    private Map<String, String> sourceConfigurations;
    private String serviceBeanName = "bomboraDepivotedService";
    private String rootOperationId;
    private List<SourceColumn> sourceColumns;
    private BomboraFirehoseInputSourceConfig inputSourceConfig;

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public Map<String, String> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @Override
    public void setSourceConfigurations(Map<String, String> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    @Override
    public InputSourceConfig getInputSourceConfig() {
        return inputSourceConfig;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String getServiceBeanName() {
        return serviceBeanName;
    }

    @Override
    public void setServiceBeanName(String serviceBeanName) {
    }

    @Override
    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    @Override
    public void setSourceColumns(List<SourceColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    @Override
    public String getRootOperationId() {
        return rootOperationId;
    }

    @Override
    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public void setBomboraFirehoseInputSourceConfig(
            BomboraFirehoseInputSourceConfig inputSourceConfig) {
        this.inputSourceConfig = inputSourceConfig;
    }
}
