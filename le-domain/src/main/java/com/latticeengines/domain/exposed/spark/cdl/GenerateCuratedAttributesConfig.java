package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateCuratedAttributesConfig extends SparkJobConfig {

    public static final String NAME = "generateCuratedAttrs";
    private static final long serialVersionUID = 7277006448211365773L;

    // [required]: column used as join key to merge attrs
    @JsonProperty
    public String joinKey;

    // input index of last activity date table
    @JsonProperty
    public Integer lastActivityDateInputIdx;

    // index of batch store table
    @JsonProperty
    public Integer masterTableIdx;

    // template name -> formated source display name
    @JsonProperty
    public Map<String, String> templateValueMap = new HashMap<>();

    /*-
     * input idx -> map of attributes to be merged (src -> attr name after merge)
     */
    @JsonProperty
    public Map<Integer, Map<String, String>> attrsToMerge = new HashMap<>();

    // input idx -> join keys, only set this if it's different than joinKey above
    @JsonProperty
    public Map<Integer, String> joinKeys = new HashMap<>();

    @Override
    public String getName() {
        return NAME;
    }
}
