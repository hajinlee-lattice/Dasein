package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

/**
 * Front-end inputs for a modeling job.
 */

public class ModelingParameters {

    @JsonProperty
    private String filename;

    @JsonProperty
    private String name;

    @JsonProperty
    private String displayName;

    @JsonProperty
    private String description;

    @JsonProperty
    private DedupType deduplicationType = DedupType.ONELEADPERDOMAIN;

    @JsonProperty
    private boolean excludePropDataColumns = false;

    @JsonProperty
    private TransformationGroup transformationGroup;

    @JsonProperty
    private String predefinedSelectionName;

    @JsonProperty
    private String moduleName;

    @JsonProperty
    private String pivotFileName;

    @JsonProperty
    public Map<String, String> runTimeParams;

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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
