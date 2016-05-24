package com.latticeengines.propdata.engine.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.engine.transformation.configuration.InputSourceConfig;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public class BomboraDepivotConfiguration implements TransformationConfiguration {
    private String sourceName;
    private String version;
    private Map<String, String> sourceConfigurations;
    private BomboraFirehoseInputSourceConfig bomboraFirehoseInputSourceConfig;
    private String serviceBeanName = "bomboraDepivotedService";
    private String rootOperationId;
    private List<SourceColumn> sourceColumns;

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public Map<String, String> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @Override
    public InputSourceConfig getInputSourceConfig(String inputSourceName) {
        return bomboraFirehoseInputSourceConfig;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public String getServiceBeanName() {
        return serviceBeanName;
    }

    @Override
    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    @Override
    public String getRootOperationId() {
        return rootOperationId;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public void setSourceConfigurations(Map<String, String> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    public void setBomboraFirehoseInputSourceConfig(BomboraFirehoseInputSourceConfig bomboraFirehoseInputSourceConfig) {
        this.bomboraFirehoseInputSourceConfig = bomboraFirehoseInputSourceConfig;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public void setSourceColumns(List<SourceColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }
}
