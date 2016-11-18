package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AdditionalEmailInfo {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("extra_info_list")
    private List<String> extraInfoList;

    @JsonProperty("extra_info_map")
    private Map<String, String> extraInfoMap;

    public AdditionalEmailInfo() {

    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public List<String> getExtraInfoList() {
        return extraInfoList;
    }

    public void setExtraInfoList(List<String> extraInfoList) {
        this.extraInfoList = extraInfoList;
    }

    public Map<String, String> getExtraInfoMap() {
        return extraInfoMap;
    }

    public void setExtraInfoMap(Map<String, String> extraInfoMap) {
        this.extraInfoMap = extraInfoMap;
    }
}
