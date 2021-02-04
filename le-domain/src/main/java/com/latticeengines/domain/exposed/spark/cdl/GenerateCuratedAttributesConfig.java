package com.latticeengines.domain.exposed.spark.cdl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateCuratedAttributesConfig extends SparkJobConfig {

    public static final String NAME = "generateCuratedAttrs";
    private static final long serialVersionUID = 7277006448211365773L;

    // : column used as join key to merge attrs
    @NotNull
    @JsonProperty
    public String joinKey;

    // input index of last activity date table
    @JsonProperty
    public Integer lastActivityDateInputIdx;

    // index of batch store table
    @NotNull
    @JsonProperty
    public Integer masterTableIdx;

    // index of parent batch store table
    @JsonProperty
    public Integer parentMasterTableIdx;

    // template name -> system display name
    @JsonProperty
    public Map<String, String> templateSystemMap = new HashMap<>();

    // template name -> entity type
    @JsonProperty
    public Map<String, String> templateTypeMap = new HashMap<>();

    // template name -> system type
    @JsonProperty
    public Map<String, String> templateSystemTypeMap = new HashMap<>();

    // columnsToIncludeFromMaster-> other attributes from master to include in final
    // table
    @JsonProperty
    public List<String> columnsToIncludeFromMaster = new ArrayList<>();

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
