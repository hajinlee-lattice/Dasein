package com.latticeengines.domain.exposed.query.frontend;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.AttributeLookup;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FrontEndSort {

    @JsonProperty("attributes")
    private List<AttributeLookup> attributes;

    @JsonProperty("descending")
    private Boolean descending;

    public FrontEndSort(List<AttributeLookup> lookups, Boolean descending) {
        this.attributes = new ArrayList<>(lookups);
        setDescending(descending);
    }

    private FrontEndSort(){}

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

}
