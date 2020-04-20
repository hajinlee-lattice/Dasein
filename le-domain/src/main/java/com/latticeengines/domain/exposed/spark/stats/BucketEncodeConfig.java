package com.latticeengines.domain.exposed.spark.stats;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.DCEncodedAttr;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class BucketEncodeConfig extends SparkJobConfig {

    public static final String NAME = "bucketEncode";

    @JsonProperty("EncAttrs")
    public List<DCEncodedAttr> encAttrs;

    @JsonProperty("RetainAttrs")
    public List<String> retainAttrs;

    @JsonProperty("RenameFields")
    public Map<String, String> renameFields;

    @JsonProperty("CodeBookMap")
    private Map<String, BitCodeBook> codeBookMap; // encoded attr -> bitCodeBook

    @JsonProperty("CodeBookLookup")
    private Map<String, String> codeBookLookup; // decoded attr -> encoded attr

    @JsonProperty("Stage")
    private String stage;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // by default, segmentation: use current

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<DCEncodedAttr> getEncAttrs() {
        return encAttrs;
    }

    public void setEncAttrs(List<DCEncodedAttr> encAttrs) {
        this.encAttrs = encAttrs;
    }

    public List<String> getRetainAttrs() {
        return retainAttrs;
    }

    public void setRetainAttrs(List<String> retainAttrs) {
        this.retainAttrs = retainAttrs;
    }

    public Map<String, String> getRenameFields() {
        return renameFields;
    }

    public void setRenameFields(Map<String, String> renameFields) {
        this.renameFields = renameFields;
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

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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
}
