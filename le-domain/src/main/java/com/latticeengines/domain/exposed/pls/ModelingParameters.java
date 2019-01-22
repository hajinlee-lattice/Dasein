package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

/**
 * Front-end inputs for a modeling job.
 */

public class ModelingParameters {

    @JsonProperty
    public Map<String, String> runTimeParams;
    @JsonProperty
    private String filename;
    @JsonProperty
    private String name;
    @JsonProperty
    private String displayName;
    @JsonProperty
    private String description;
    @JsonProperty
    private String userId;
    @JsonProperty
    private DedupType deduplicationType = DedupType.MULTIPLELEADSPERDOMAIN;
    @JsonProperty
    private boolean excludePropDataColumns; // LDC or no LDC
    @JsonProperty
    private boolean excludeCDLAttributes;
    @JsonProperty
    private boolean excludeCustomFileAttributes;
    @JsonProperty
    private boolean excludePublicDomains = false;
    @JsonProperty
    private TransformationGroup transformationGroup;
    @JsonProperty
    private String predefinedSelectionName;
    @JsonProperty
    private String selectedVersion;
    @JsonProperty
    private String moduleName;
    @JsonProperty
    private String pivotFileName;
    @JsonProperty
    private String dataCloudVersion;

    @JsonProperty
    private String notesContent;

    @JsonProperty
    private boolean activateModelSummaryByDefault = false;

    @JsonProperty
    private CustomEventModelingType customEventModelingType = CustomEventModelingType.LPI;

    @JsonProperty
    private EntityType cdlEntityType;

    @JsonProperty
    private String aiModelId;

    @JsonProperty
    private String ratingEngineId;

    @JsonProperty
    private Map<String, ColumnMetadata> userRefinedAttributes;

    @JsonProperty
    private Integer modelIteration;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public DedupType getDeduplicationType() {
        return deduplicationType;
    }

    public void setDeduplicationType(DedupType deduplicationType) {
        this.deduplicationType = deduplicationType;
    }

    public boolean getExcludePropDataColumns() {
        return excludePropDataColumns;
    }

    public void setExcludePropDataColumns(boolean excludePropDataColumns) {
        this.excludePropDataColumns = excludePropDataColumns;
    }

    public boolean isExcludePublicDomains() {
        return excludePublicDomains;
    }

    public void setExcludePublicDomains(boolean excludePublicDomains) {
        this.excludePublicDomains = excludePublicDomains;
    }

    public TransformationGroup getTransformationGroup() {
        return transformationGroup;
    }

    public void setTransformationGroup(TransformationGroup transformationGroup) {
        this.transformationGroup = transformationGroup;
    }

    public String getPredefinedSelectionName() {
        return predefinedSelectionName;
    }

    public void setPredefinedSelectionName(String predefinedSelectionName) {
        this.predefinedSelectionName = predefinedSelectionName;
    }

    public String getSelectedVersion() {
        return selectedVersion;
    }

    public void setSelectedVersion(String selectedVersion) {
        this.selectedVersion = selectedVersion;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getPivotFileName() {
        return pivotFileName;
    }

    public void setPivotFileName(String pivotFileName) {
        this.pivotFileName = pivotFileName;
    }

    public Map<String, String> getRunTimeParams() {
        return runTimeParams;
    }

    public void setRunTimeParams(Map<String, String> runTimeParams) {
        this.runTimeParams = runTimeParams;
    }

    public String getNotesContent() {
        return notesContent;
    }

    public void setNotesContent(String notesContent) {
        this.notesContent = notesContent;
    }

    public boolean getActivateModelSummaryByDefault() {
        return this.activateModelSummaryByDefault;
    }

    public void setActivateModelSummaryByDefault(boolean value) {
        this.activateModelSummaryByDefault = value;
    }

    public CustomEventModelingType getCustomEventModelingType() {
        return this.customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }

    public EntityType getCdlEntityType() {
        return cdlEntityType;
    }

    public void setCdlEntityType(EntityType cdlEntityType) {
        this.cdlEntityType = cdlEntityType;
    }

    public String getAiModelId() {
        return aiModelId;
    }

    public void setAiModelId(String aiModelId) {
        this.aiModelId = aiModelId;
    }

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public boolean isExcludeCDLAttributes() {
        return excludeCDLAttributes;
    }

    public void setExcludeCDLAttributes(boolean excludeCDLAttributes) {
        this.excludeCDLAttributes = excludeCDLAttributes;
    }

    public boolean isExcludeCustomFileAttributes() {
        return excludeCustomFileAttributes;
    }

    public void setExcludeCustomFileAttributes(boolean excludeCustomFileAttributes) {
        this.excludeCustomFileAttributes = excludeCustomFileAttributes;
    }

    public Map<String, ColumnMetadata> getUserRefinedAttributes() {
        return userRefinedAttributes;
    }

    public void setUserRefinedAttributes(Map<String, ColumnMetadata> userRefinedAttributes) {
        this.userRefinedAttributes = userRefinedAttributes;
    }

    public Integer getModelIteration() {
        return modelIteration;
    }

    public void setModelIteration(Integer modelIteration) {
        this.modelIteration = modelIteration;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
