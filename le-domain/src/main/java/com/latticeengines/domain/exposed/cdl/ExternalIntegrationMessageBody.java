package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;

public class ExternalIntegrationMessageBody {

    // Tray User ID
    private String trayTenantId;

    private Map<String, List<ExportFileConfig>> sourceFiles;

    private Map<String, List<ExportFileConfig>> deleteFiles;

    private Map<String, List<ExportFileConfig>> taskDescription;

    private String externalAudienceId;

    private String externalAudienceName;

    private String workflowRequestId;

    private String solutionInstanceId;

    private String folderId;

    private String folderName;

    private String audienceType;

    private String stackName;

    private String stack;

    private Boolean enableAcxiom;

    private String taskPriority;

    private String taskType;

    private String taskOwnerPriority;

    private String taskDefaultOwner;

    public String getTrayTenantId() {
        return trayTenantId;
    }

    public void setTrayTenantId(String trayTenantId) {
        this.trayTenantId = trayTenantId;
    }

    public Map<String, List<ExportFileConfig>> getSourceFiles() {
        return sourceFiles;
    }

    public void setSourceFiles(Map<String, List<ExportFileConfig>> sourceFiles) {
        this.sourceFiles = sourceFiles;
    }

    public Map<String, List<ExportFileConfig>> getDeleteFiles() {
        return deleteFiles;
    }

    public void setDeleteFiles(Map<String, List<ExportFileConfig>> deleteFiles) {
        this.deleteFiles = deleteFiles;
    }

    public Map<String, List<ExportFileConfig>> getTaskDescription() {
        return taskDescription;
    }

    public void setTaskDescription(Map<String, List<ExportFileConfig>> taskDescription) {
        this.taskDescription = taskDescription;
    }

    public String getExternalAudienceId() {
        return externalAudienceId;
    }

    public void setExternalAudienceId(String externalAudienceId) {
        this.externalAudienceId = externalAudienceId;
    }

    public String getExternalAudienceName() {
        return externalAudienceName;
    }

    public void setExternalAudienceName(String externalAudienceName) {
        this.externalAudienceName = externalAudienceName;
    }

    public String getWorkflowRequestId() {
        return workflowRequestId;
    }

    public void setWorkflowRequestId(String workflowRequestId) {
        this.workflowRequestId = workflowRequestId;
    }

    public String getSolutionInstanceId() {
        return solutionInstanceId;
    }

    public void setSolutionInstanceId(String solutionInstanceId) {
        this.solutionInstanceId = solutionInstanceId;
    }

    public void setFolderId(String folderId) {
        this.folderId = folderId;
    }

    public String getFolderId() {
        return folderId;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setAudienceType(String audienceType) {
        this.audienceType = audienceType;
    }

    public String getAudienceType() {
        return audienceType;
    }

    public void setStackName(String stackName) {
        this.stackName = stackName;
    }

    public String getStackName() {
        return stackName;
    }

    public String getStack() { return stack; }

    public void setStack(String stack) { this.stack = stack; }

    public boolean getEnableAcxiom() { return enableAcxiom; }

    public void setEnableAcxiom(boolean enableAcxiom) { this.enableAcxiom = enableAcxiom; }

    public void setTaskPriority(String taskPriority) {
        this.taskPriority = taskPriority;
    }

    public String getTaskPriority() {
        return taskPriority;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskOwnerPriority(String taskOwnerPriority) {
        this.taskOwnerPriority = taskOwnerPriority;
    }

    public String getTaskOwnerPriority() {
        return taskOwnerPriority;
    }

    public void setTaskDefaultOwner(String taskDefaultOwner) {
        this.taskDefaultOwner = taskDefaultOwner;
    }

    public String getTaskDefaultOwner() {
        return taskDefaultOwner;
    }
}
