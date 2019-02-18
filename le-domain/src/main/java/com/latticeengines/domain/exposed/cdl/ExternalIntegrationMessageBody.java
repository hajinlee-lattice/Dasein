package com.latticeengines.domain.exposed.cdl;

import java.util.List;

public class ExternalIntegrationMessageBody {

    // Tray User ID
    private String trayTenantId;

    private List<ExportFileConfig> sourceFiles;

    private String externalAudienceId;

    private String workflowRequestId;

    private String solutionInstanceId;

    public String getTrayTenantId() {
        return trayTenantId;
    }

    public void setTrayTenantId(String trayTenantId) {
        this.trayTenantId = trayTenantId;
    }

    public List<ExportFileConfig> getSourceFiles() {
        return sourceFiles;
    }

    public void setSourceFiles(List<ExportFileConfig> sourceFiles) {
        this.sourceFiles = sourceFiles;
    }

    public String getExternalAudienceId() {
        return externalAudienceId;
    }

    public void setExternalAudienceId(String externalAudienceId) {
        this.externalAudienceId = externalAudienceId;
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

}
