package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelStepConfiguration extends MicroserviceStepConfiguration {
    @NotEmptyString
    @NotNull
    private String modelingServiceHdfsBaseDir;

    @NotEmptyString
    @NotNull
    private String modelName;

    private String displayName;

    private String eventTableName;

    private String productType;

    private String sourceSchemaInterpretation;

    private String trainingTableName;

    private String transformationGroupName;

    private ModelSummary sourceModelSummary;

    private ModelSummaryProvenance modelSummaryProvenance = new ModelSummaryProvenance();

    private String pivotArtifactPath;

    private Map<String, String> runTimeParams;

    private boolean defaultDataRuleConfiguration;

    private List<DataRule> dataRules;

    private String dataCloudVersion;

    private String moduleName;

    private boolean v2ProfilingEnabled;

    private boolean isCdlModel = false;

    private String notesContent;

    private String userName;

    private boolean activateModelSummaryByDefault = false;

    @JsonProperty
    public List<DataRule> getDataRules() {
        return dataRules;
    }

    @JsonProperty
    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
    }

    @JsonProperty
    public boolean isDefaultDataRuleConfiguration() {
        return defaultDataRuleConfiguration;
    }

    @JsonProperty
    public void setDefaultDataRuleConfiguration(boolean defaultDataRuleConfiguration) {
        this.defaultDataRuleConfiguration = defaultDataRuleConfiguration;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

    @JsonProperty
    public String getModelName() {
        return modelName;
    }

    @JsonProperty
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    @JsonProperty
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public String getTrainingTableName() {
        return trainingTableName;
    }

    public void setTrainingTableName(String trainingTableName) {
        this.trainingTableName = trainingTableName;
    }

    public String getTransformationGroupName() {
        return transformationGroupName;
    }

    public void setTransformationGroupName(String transformationGroupName) {
        this.transformationGroupName = transformationGroupName;
    }

    public ModelSummary getSourceModelSummary() {
        return sourceModelSummary;
    }

    public void setSourceModelSummary(ModelSummary sourceModelSummary) {
        this.sourceModelSummary = sourceModelSummary;
    }

    @JsonProperty("modelSummaryProvenance")
    public ModelSummaryProvenance getModelSummaryProvenance() {
        return modelSummaryProvenance;
    }

    @JsonProperty("modelSummaryProvenance")
    public void setModelSummaryProvenance(ModelSummaryProvenance modelSummaryProvenance) {
        this.modelSummaryProvenance = modelSummaryProvenance;
    }

    public void addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
        modelSummaryProvenance.setProvenanceProperty(propertyName, value);
    }

    @JsonProperty
    public String getPivotArtifactPath() {
        return pivotArtifactPath;
    }

    @JsonProperty
    public void setPivotArtifactPath(String pivotArtifactPath) {
        this.pivotArtifactPath = pivotArtifactPath;
    }

    @JsonProperty
    public Map<String, String> getRunTimeParams() {
        return this.runTimeParams;
    }

    @JsonProperty
    public void setRunTimeParams(Map<String, String> runTimeParams) {
        this.runTimeParams = runTimeParams;
    }

    @JsonProperty
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @JsonProperty
    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    @JsonProperty
    public boolean isV2ProfilingEnabled() {
        return v2ProfilingEnabled;
    }

    public void setV2ProfilingEnabled(boolean v2ProfilingEnabled) {
        this.v2ProfilingEnabled = v2ProfilingEnabled;
    }

    @JsonProperty
    public boolean isCdlModel() {
        return isCdlModel;
    }

    public void setCdlModel(boolean isCdlModel) {
        this.isCdlModel = isCdlModel;
    }

    @JsonProperty
    public String getNotesContent() {
        return notesContent;
    }

    @JsonProperty
    public void setNotesContent(String notesContent) {
        this.notesContent = notesContent;
    }

    @JsonProperty
    public String getUserName() {
        return userName;
    }

    @JsonProperty
    public void setUserName(String userName) {
        this.userName = userName;
    }

    @JsonProperty
    public boolean getActivateModelSummaryByDefault() {
        return this.activateModelSummaryByDefault;
    }

    @JsonProperty
    public void setActivateModelSummaryByDefault(boolean activateModelSummaryByDefault) {
        this.activateModelSummaryByDefault = activateModelSummaryByDefault;
    }

}
