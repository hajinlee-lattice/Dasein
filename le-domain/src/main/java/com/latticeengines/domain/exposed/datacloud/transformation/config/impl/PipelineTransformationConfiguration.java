package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.InputSourceConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class PipelineTransformationConfiguration extends BasicTransformationConfiguration
        implements TransformationConfiguration {

    private String name;
    private String sourceName;
    private String version;
    private String serviceBeanName;

    private Integer containerMemMB;

    private Map<String, String> sourceConfigurations;
    private String rootOperationId;
    private List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
    private InputSourceConfig inputSourceConfig = null;

    private List<TransformationStepConfig> steps;

    private boolean keepTemp;
    private boolean enableSlack;

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
        this.serviceBeanName = serviceBeanName;
    }

    @Override
    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    @Override
    public void setSourceColumns(List<SourceColumn> sourceColumns) {
    }

    @Override
    public String getRootOperationId() {
        return rootOperationId;
    }

    @Override
    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean getKeepTemp() {
        return keepTemp;
    }

    public void setKeepTemp(boolean keepTemp) {
        this.keepTemp = keepTemp;
    }

    public boolean isEnableSlack() {
        return enableSlack;
    }

    public void setEnableSlack(boolean enableSlack) {
        this.enableSlack = enableSlack;
    }

    public Integer getContainerMemMB() {
        return containerMemMB;
    }

    public void setContainerMemMB(Integer containerMemMB) {
        this.containerMemMB = containerMemMB;
    }
}
