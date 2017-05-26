package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modelreview.DataRule;

public class CloneModelingParameters {
    @JsonProperty
    private String sourceModelSummaryId;

    @JsonProperty
    private String name;

    @JsonProperty
    private String displayName;

    @JsonProperty
    private String description;

    @JsonProperty
    private String userId;

    @JsonProperty
    private List<VdbMetadataField> attributes;

    @JsonProperty
    private DedupType deduplicationType = DedupType.MULTIPLELEADSPERDOMAIN;

    @JsonProperty
    private List<DataRule> dataRules;

    @JsonProperty
    private boolean excludeUnmatchedWithPublicDomain = false;

    @JsonProperty
    private boolean excludePropDataAttributes = false;

    @JsonProperty
    private boolean enableTransformations;

    @JsonProperty
    private String notesContent;

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

    public List<VdbMetadataField> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<VdbMetadataField> attributes) {
        this.attributes = attributes;
    }

    public String getSourceModelSummaryId() {
        return sourceModelSummaryId;
    }

    public void setSourceModelSummaryId(String sourceModelSummaryId) {
        this.sourceModelSummaryId = sourceModelSummaryId;
    }

    public DedupType getDeduplicationType() {
        return deduplicationType;
    }

    public void setDeduplicationType(DedupType deduplicationType) {
        this.deduplicationType = deduplicationType;
    }

    public List<DataRule> getDataRules() {
        return dataRules;
    }

    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
    }

    public boolean isExcludeUnmatchedWithPublicDomain() {
        return excludeUnmatchedWithPublicDomain;
    }

    public void setExcludeUnmatchedWithPublicDomain(boolean excludeUnmatchedWithPublicDomain) {
        this.excludeUnmatchedWithPublicDomain = excludeUnmatchedWithPublicDomain;
    }

    public boolean isExcludePropDataAttributes() {
        return excludePropDataAttributes;
    }

    public void setExcludePropDataAttributes(boolean excludePropDataAttributes) {
        this.excludePropDataAttributes = excludePropDataAttributes;
    }

    public boolean enableTransformation() {
        return enableTransformations;
    }

    public void setEnableTransformations(boolean enableTransformations) {
        this.enableTransformations = enableTransformations;
    }

    public String getNotesContent() {
        return notesContent;
    }

    public void setNotesContent(String notesContent) {
        this.notesContent = notesContent;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
