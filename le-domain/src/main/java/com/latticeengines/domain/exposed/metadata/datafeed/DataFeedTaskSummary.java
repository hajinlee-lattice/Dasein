package com.latticeengines.domain.exposed.metadata.datafeed;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DataFeedTaskSummary {

    @JsonProperty("source")
    private String source;

    @JsonProperty("entity")
    private String entity;

    @JsonProperty("feed_type")
    private String feedType;

    @JsonProperty("subtype")
    private DataFeedTask.SubType subtype;

    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("template_display_name")
    private String templateDisplayName;

    @JsonProperty("s3_import_status")
    private DataFeedTask.S3ImportStatus s3ImportStatus;

    @JsonProperty("last_updated")
    private Date lastUpdated;

    @JsonProperty("spec_type")
    private String specType;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public DataFeedTask.SubType getSubtype() {
        return subtype;
    }

    public void setSubtype(DataFeedTask.SubType subtype) {
        this.subtype = subtype;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getTemplateDisplayName() {
        return templateDisplayName;
    }

    public void setTemplateDisplayName(String templateDisplayName) {
        this.templateDisplayName = templateDisplayName;
    }

    public DataFeedTask.S3ImportStatus getS3ImportStatus() {
        return s3ImportStatus;
    }

    public void setS3ImportStatus(DataFeedTask.S3ImportStatus s3ImportStatus) {
        this.s3ImportStatus = s3ImportStatus;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public String getSpecType() {
        return specType;
    }

    public void setSpecType(String specType) {
        this.specType = specType;
    }
}
