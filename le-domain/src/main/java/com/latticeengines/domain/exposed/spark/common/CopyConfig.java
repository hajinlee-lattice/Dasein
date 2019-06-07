package com.latticeengines.domain.exposed.spark.common;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

// simple modification of columns
// can also concatenate multiple inputs
public class CopyConfig extends SparkJobConfig {

    public static final String NAME = "copy";

    // operation sequence: select -> drop -> rename -> concatenate
    @JsonProperty("SelectAttrs")
    private List<String> selectAttrs;

    @JsonProperty("DropAttrs")
    private List<String> dropAttrs;

    @JsonProperty("RenameAttrs")
    private Map<String, String> renameAttrs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getSelectAttrs() {
        return selectAttrs;
    }

    public void setSelectAttrs(List<String> selectAttrs) {
        this.selectAttrs = selectAttrs;
    }

    public List<String> getDropAttrs() {
        return dropAttrs;
    }

    public void setDropAttrs(List<String> dropAttrs) {
        this.dropAttrs = dropAttrs;
    }

    public Map<String, String> getRenameAttrs() {
        return renameAttrs;
    }

    public void setRenameAttrs(Map<String, String> renameAttrs) {
        this.renameAttrs = renameAttrs;
    }
}
