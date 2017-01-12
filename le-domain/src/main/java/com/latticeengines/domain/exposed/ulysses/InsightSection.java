package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import java.util.UUID;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class InsightSection implements HasId<String> {

    @JsonProperty("id")
    private String id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("headline")
    private String headline;

    @JsonProperty("tip")
    private String tip;

    @JsonProperty("attributes")
    private List<String> attributes = new ArrayList<>();

    @JsonProperty("insight_source_type")
    private InsightSourceType insightSourceType;

    public InsightSection() {
        id = UUID.randomUUID().toString();
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    public String getTip() {
        return tip;
    }

    public void setTip(String tip) {
        this.tip = tip;
    }

    public InsightSourceType getInsightSourceType() {
        return insightSourceType;
    }

    public void setInsightSourceType(InsightSourceType insightSourceType) {
        this.insightSourceType = insightSourceType;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }
}
