package com.latticeengines.domain.exposed.spark.stats;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalcStatsConfig extends SparkJobConfig {

    public static final String NAME = "calcStats";

    @JsonProperty("UseChangeList")
    private boolean useChangeList;

    // =========================
    // BEGIN: for AM rebuild
    // =========================
    @JsonProperty("DimensionTree")
    private Map<String, List<String>> dimensionTree;

    @JsonProperty("DedupFields")
    private List<String> dedupFields;

    @JsonProperty("CodeBookMap")
    private Map<String, BitCodeBook> codeBookMap; // encoded attr -> bitCodeBook

    @JsonProperty("CodeBookLookup")
    private Map<String, String> codeBookLookup; // decoded attr -> encoded attr

    @JsonProperty("Stage")
    private String stage;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;
    // =========================
    // END: for AM rebuild
    // =========================

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public Map<String, List<String>> getDimensionTree() {
        return dimensionTree;
    }

    public void setDimensionTree(Map<String, List<String>> dimensionTree) {
        this.dimensionTree = dimensionTree;
    }

    public List<String> getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(List<String> dedupFields) {
        this.dedupFields = dedupFields;
    }

    public boolean isUseChangeList() {
        return useChangeList;
    }

    public void setUseChangeList(boolean useChangeList) {
        this.useChangeList = useChangeList;
    }

    public Map<String, BitCodeBook> getCodeBookMap() {
        return codeBookMap;
    }

    public void setCodeBookMap(Map<String, BitCodeBook> codeBookMap) {
        this.codeBookMap = codeBookMap;
    }

    public Map<String, String> getCodeBookLookup() {
        return codeBookLookup;
    }

    public void setCodeBookLookup(Map<String, String> codeBookLookup) {
        this.codeBookLookup = codeBookLookup;
    }

    public String getStage() {
        if (StringUtils.isBlank(stage)) {
            setStage(DataCloudConstants.PROFILE_STAGE_SEGMENT);
        }
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }
}
