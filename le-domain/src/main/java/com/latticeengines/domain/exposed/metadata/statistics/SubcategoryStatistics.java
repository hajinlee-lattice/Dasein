package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.query.AttributeLookup;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SubcategoryStatistics {

    @JsonProperty("Attributes")
    @JsonDeserialize(keyUsing = AttributeLookup.AttributeLookupKeyDeserializer.class)
    private Map<AttributeLookup, AttributeStats> attributes = new HashMap<>();

    public Map<AttributeLookup, AttributeStats> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<AttributeLookup, AttributeStats> attributes) {
        this.attributes = attributes;
    }

    public AttributeStats getAttrStats(AttributeLookup lookup) {
        return attributes.get(lookup);
    }

    public void putAttrStats(AttributeLookup lookup, AttributeStats stats) {
        attributes.put(lookup, stats);
    }

    public boolean hasAttrStats(AttributeLookup lookup) {
        return attributes.containsKey(lookup);
    }

}
