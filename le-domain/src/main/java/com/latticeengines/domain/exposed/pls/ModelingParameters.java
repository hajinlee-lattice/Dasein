package com.latticeengines.domain.exposed.pls;

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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
