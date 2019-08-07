package com.latticeengines.domain.exposed.cdl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ConvertBatchStoreToImportRequest {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("template_name")
    private String templateName;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("feed_type")
    private String feedType;

    @JsonProperty("sub_type")
    private String subType;

    @JsonProperty("rename_map")
    private Map<String, String> renameMap;

    @JsonProperty("duplicate_map")
    private Map<String, String> duplicateMap;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getSubType() {
        return subType;
    }

    public void setSubType(String subType) {
        this.subType = subType;
    }

    public Map<String, String> getRenameMap() {
        return renameMap;
    }

    public void setRenameMap(Map<String, String> renameMap) {
        this.renameMap = renameMap;
    }

    public Map<String, String> getDuplicateMap() {
        return duplicateMap;
    }

    public void setDuplicateMap(Map<String, String> duplicateMap) {
        this.duplicateMap = duplicateMap;
    }
}
