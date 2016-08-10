package com.latticeengines.domain.exposed.propdata.match;

import javax.persistence.Id;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class LatticeAccount implements HasId<String> {
    @Id
    @JsonProperty("lattice_account_id")
    private String id;

    @JsonProperty("attributes")
    private Map<String, String> attributes;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
