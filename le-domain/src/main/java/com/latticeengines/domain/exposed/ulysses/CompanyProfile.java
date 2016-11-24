package com.latticeengines.domain.exposed.ulysses;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CompanyProfile {

    @JsonProperty("attributes")
    public Map<String, String> attributes = new HashMap<>();
}
