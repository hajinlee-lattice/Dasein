package com.latticeengines.propdata.engine.transformation.configuration.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.propdata.engine.transformation.configuration.InputSourceConfig;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public class BasicTransformationConfiguration implements TransformationConfiguration {

    private String sourceName;
    private String serviceBeanName;
    private String version;
    private Map<String, String> sourceConfigurations;
    private String rootOperationId;
    private List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
    private InputSourceConfig inputSourceConfig = null;

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
    }

    @Override
    public String getServiceBeanName() {
        return serviceBeanName;
    }

    @Override
    public void setServiceBeanName(String serviceBeanName) {
        this.serviceBeanName = serviceBeanName;
    }

    @Override
    public Map<String, String> getSourceConfigurations() {
        return sourceConfigurations;
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
    public String getRootOperationId() {
        return rootOperationId;
    }

    @Override
    public void setSourceConfigurations(Map<String, String> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    @Override
    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    @Override
    public void setSourceColumns(List<SourceColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    @Override
    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

}
