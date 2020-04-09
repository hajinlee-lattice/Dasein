package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeCuratedAttributesConfig extends SparkJobConfig {

    public static final String NAME = "mergeCuratedAttrs";
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

    /*-
     * input idx -> list of attributes to be merged
     */
    @JsonProperty
    public Map<Integer, List<String>> attrsToMerge = new HashMap<>();

    @Override
    public String getName() {
        return NAME;
    }
}
