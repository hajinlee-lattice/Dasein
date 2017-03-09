package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbLoadTableCancel {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("launch_id")
    private int launchId;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("vdb_query_handle")
    private String vdbQueryHandle;

    @JsonProperty("report_status_endpoint")
    private String reportStatusEndpoint;

    public String getVdbQueryHandle() {
        return vdbQueryHandle;
    }

    public void setVdbQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    public String getReportStatusEndpoint() {
        return reportStatusEndpoint;
    }

    public void setReportStatusEndpoint(String reportStatusEndpoint) {
        this.reportStatusEndpoint = reportStatusEndpoint;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public int getLaunchId() {
        return launchId;
    }

    public void setLaunchId(int launchId) {
        this.launchId = launchId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
