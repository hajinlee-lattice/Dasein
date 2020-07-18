package com.latticeengines.domain.exposed.spark.am;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public abstract class TblDrivenTxfmrConfig extends SparkJobConfig {

    public static final String NAME = "tblDrivenTxfmrConfig";

    @JsonProperty("Source")
    private String source;

    @JsonProperty("Stage")
    private String stage;

    @JsonProperty("Templates")
    private List<String> templates;

    @JsonProperty("BaseTables")
    private List<String> baseTables;

    public String getSource() {
        return source;
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public List<String> getTemplates() {
        return templates;
    }

    public void setTemplates(List<String> templates) {
        this.templates = templates;
    }

    public List<String> getBaseTables() {
        return baseTables;
    }

    public void setBaseTables(List<String> baseTables) {
        this.baseTables = baseTables;
    }
}
