package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbLoadTableConfig {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("launch_id")
    private String launchId;

    @JsonProperty("data_category")
    private String dataCategory;

    @JsonProperty("merge_rule")
    private String mergeRule;

    @JsonProperty("create_table_rule")
    private String createTableRule;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("vdb_query_handle")
    private String vdbQueryHandle;

    @JsonProperty("total_rows")
    private int totalRows;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("get_query_data_endpoint")
    private String getQueryDataEndpoint;

    @JsonProperty("report_status_endpoint")
    private String reportStatusEndpoint;

    @JsonProperty("vdb_spec_metadata")
    private List<VdbSpecMetadata> metadataList;

    public List<VdbSpecMetadata> getMetadataList() {
        return metadataList;
    }

    public void setMetadataList(List<VdbSpecMetadata> metadataList) {
        this.metadataList = metadataList;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

    public String getDataCategory() {
        return dataCategory;
    }

    public void setDataCategory(String dataCategory) {
        this.dataCategory = dataCategory;
    }

    public String getMergeRule() {
        return mergeRule;
    }

    public void setMergeRule(String mergeRule) {
        this.mergeRule = mergeRule;
    }

    public String getCreateTableRule() {
        return createTableRule;
    }

    public void setCreateTableRule(String createTableRule) {
        this.createTableRule = createTableRule;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getVdbQueryHandle() {
        return vdbQueryHandle;
    }

    public void setVdbQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getGetQueryDataEndpoint() {
        return getQueryDataEndpoint;
    }

    public void setGetQueryDataEndpoint(String getQueryDataEndpoint) {
        this.getQueryDataEndpoint = getQueryDataEndpoint;
    }

    public String getReportStatusEndpoint() {
        return reportStatusEndpoint;
    }

    public void setReportStatusEndpoint(String reportStatusEndpoint) {
        this.reportStatusEndpoint = reportStatusEndpoint;
    }
}
