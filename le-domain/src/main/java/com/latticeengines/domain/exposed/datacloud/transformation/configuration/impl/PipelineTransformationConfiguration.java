package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.InputSourceConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;

public class PipelineTransformationConfiguration extends BasicTransformationConfiguration implements TransformationConfiguration {

    private String sourceName;
    private String version;
    private String serviceBeanName;

    private Map<String, String> sourceConfigurations;
    private String rootOperationId;
    private List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
    private InputSourceConfig inputSourceConfig = null;

    private List<String> baseSources;
    private List<String> baseVersions;
    private List<String> baseTemplates;
    private String  targetSource;
    private String  targetTemplate;
    private String  targetVersion;
    private List<TransformationStepConfig> steps;

    @Override
    public String getSourceName() {
        return sourceName;
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
    public String getRootOperationId() {
        return rootOperationId;
    }

    @Override
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
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
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }

    public void setBaseSources(List<String> baseSources) {
        this.baseSources = baseSources;
    }

    public List<String> getBaseSources() {
        return baseSources;
    }

    public void setBaseTemplates(List<String> baseTemplates) {
        this.baseTemplates = baseTemplates;
    }

    public List<String> getBaseTemplates() {
        return baseTemplates;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setTargetSource(String targetSource) {
        this.targetSource = targetSource;
    }

    public String getTargetSource() {
        return targetSource;
    }

    public void setTargetTemplate(String targetTemplate) {
        this.targetTemplate = targetTemplate;
    }

    public String getTargetTemplate() {
        return targetTemplate;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public String getTargetVersion() {
        return targetVersion;
    }
}
