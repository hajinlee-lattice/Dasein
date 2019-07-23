package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;

public class ExternalIntegrationMessageBody {

    // Tray User ID
    private String trayTenantId;

    private Map<String, List<ExportFileConfig>> sourceFiles;

    private Map<String, List<ExportFileConfig>> deleteFiles;

    private String externalAudienceId;

    private String externalAudienceName;

    private String workflowRequestId;

    private String solutionInstanceId;

    private String folderName;

    private String audienceType;

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
}
