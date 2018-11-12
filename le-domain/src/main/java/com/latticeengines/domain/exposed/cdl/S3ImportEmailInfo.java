package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.EntityType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class S3ImportEmailInfo {

    @JsonProperty("tenant_name")
    private String tenantName;

    @JsonProperty("template_name")
    private String templateName;

    @JsonProperty("entity_type")
    private EntityType entityType;

    @JsonProperty("drop_folder")
    private String dropFolder;

    @JsonProperty("file_name")
    private String fileName;

    @JsonProperty("time_received")
    private Date timeReceived;

    @JsonProperty("error_msg")
    private String errorMsg;

    @JsonProperty("user")
    private String user;

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public String getDropFolder() {
        return dropFolder;
    }

    public void setDropFolder(String dropFolder) {
        this.dropFolder = dropFolder;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Date getTimeReceived() {
        return timeReceived;
    }

    public void setTimeReceived(Date timeReceived) {
        this.timeReceived = timeReceived;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
