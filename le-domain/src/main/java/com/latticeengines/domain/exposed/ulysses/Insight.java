package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

import java.util.UUID;
import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Insight implements HasName, HasId<String> {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("insight_sections")
    private List<InsightSection> insightSections = new ArrayList<>();

    public Insight() {
        id = UUID.randomUUID().toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public List<InsightSection> getInsightSections() {
        return insightSections;
    }

    public void setInsightSections(List<InsightSection> insightSections) {
        this.insightSections = insightSections;
    }

}
