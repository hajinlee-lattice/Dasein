package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class S3ImportTemplateDisplay {

    @JsonProperty("Object")
    private String object;
    @JsonProperty("Path")
    private String path;
    @JsonProperty("TemplateName")
    private String templateName;
    @JsonProperty("LastEditedDate")
    private Date lastEditedDate;
    @JsonProperty("Exist")
    private Boolean exist;
    @JsonProperty("FeedType")
    private String feedType;
    @JsonProperty("SystemName")
    private String systemName;
    @JsonProperty("ImportStatus")
    private DataFeedTask.S3ImportStatus importStatus;
    @JsonProperty("Entity")
    private BusinessEntity entity;

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public Date getLastEditedDate() {
        return lastEditedDate;
    }

    public void setLastEditedDate(Date lastEditedDate) {
        this.lastEditedDate = lastEditedDate;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public Boolean getExist() {
        return exist;
    }

    public void setExist(Boolean exist) {
        this.exist = exist;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public DataFeedTask.S3ImportStatus getImportStatus() {
        return importStatus;
    }

    public void setImportStatus(DataFeedTask.S3ImportStatus importStatus) {
        this.importStatus = importStatus;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }
}
