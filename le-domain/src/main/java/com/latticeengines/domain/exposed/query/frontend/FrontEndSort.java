package com.latticeengines.domain.exposed.query.frontend;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.AttributeLookup;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FrontEndSort {

    @JsonIgnore
    private List<AttributeLookup> attributes;

    @JsonProperty("descending")
    private Boolean descending;

    public FrontEndSort(List<AttributeLookup> lookups, Boolean descending) {
        this.attributes = lookups;
        setDescending(descending);
    }

    public List<AttributeLookup> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<AttributeLookup> attributes) {
        this.attributes = attributes;
    }

    public Boolean getDescending() {
        if (Boolean.TRUE.equals(descending)){
            return true;
        } else {
            descending = null;
            return null;
        }
    }

    public void setDescending(Boolean descending) {
        this.descending = Boolean.TRUE.equals(descending) ? true : null;
    }

    @JsonProperty("attrs")
    private List<String> getAttrsAsStrings() {
        if (attributes == null) {
            return null;
        }
        return attributes.stream().map(AttributeLookup::toString).collect(Collectors.toList());
    }

    @JsonProperty("attrs")
    private void setAttrsViaStrings(List<String> tokens) {
        if (tokens == null) {
            this.attributes = null;
        } else {
            this.attributes = tokens.stream().map(AttributeLookup::fromString).collect(Collectors.toList());
        }
    }
}
