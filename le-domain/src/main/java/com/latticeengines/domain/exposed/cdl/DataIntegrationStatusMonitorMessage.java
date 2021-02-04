package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIntegrationStatusMonitorMessage {

    @JsonProperty("tenantName")
    private String tenantName;

    @JsonProperty("workflowRequestId")
    private String workflowRequestId;

    @JsonProperty("operation")
    private String operation;

    @JsonProperty("entityName")
    private String entityName;

    @JsonProperty("entityId")
    private String entityId;

    @JsonProperty("externalSystemId")
    private String externalSystemId;

    @JsonProperty("sourceFile")
    private String sourceFile;

    @JsonProperty("deleteFile")
    private String deleteFile;

    @JsonProperty("messageType")
    private String messageType;

    @JsonProperty("message")
    private String message;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("eventTime")
    private Date eventTime;

    @JsonProperty("eventDetail")
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "eventType")
    @JsonSubTypes({ //
            @Type(value = ProgressEventDetail.class, name = "Progress"), //
            @Type(value = ProgressEventDetail.class, name = "Completed"),
            @Type(value = ProgressEventDetail.class, name = "InProgress"),
            @Type(value = AudienceEventDetail.class, name = "AudienceCreation"),
            @Type(value = AudienceEventDetail.class, name = "AudienceSizeUpdate"),
            @Type(value = FailedEventDetail.class, name = "Failed"),
            @Type(value = InitiatedEventDetail.class, name = "Initiated"),
            @Type(value = AuthInvalidatedEventDetail.class, name = "AuthInvalidated") })
    private EventDetail eventDetail;

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getExternalSystemId() {
        return externalSystemId;
    }

    public void setExternalSystemId(String externalSystemId) {
        this.externalSystemId = externalSystemId;
    }

    public String getWorkflowRequestId() {
        return workflowRequestId;
    }

    public void setWorkflowRequestId(String workflowRequestId) {
        this.workflowRequestId = workflowRequestId;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public String getDeleteFile() {
        return deleteFile;
    }

    public void setDeleteFile(String deleteFile) {
        this.deleteFile = deleteFile;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public EventDetail getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(EventDetail eventDetail) {
        this.eventDetail = eventDetail;
    }
}
