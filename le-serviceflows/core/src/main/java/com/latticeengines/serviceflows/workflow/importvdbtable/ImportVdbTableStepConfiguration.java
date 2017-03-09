package com.latticeengines.serviceflows.workflow.importvdbtable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

import java.util.List;

public class ImportVdbTableStepConfiguration extends BaseStepConfiguration {

    @NotNull
    private CustomerSpace customerSpace;

    @NotNull
    private String tableName;

    @NotNull
    private String getQueryDataEndpoint;

    private String extractIdentifier;

    private String dataCategory;

    private String vdbQueryHandle;

    private int totalRows;

    private String reportStatusEndpoint;

    private String mergeRule;

    private VdbCreateTableRule createTableRule;

    private List<VdbSpecMetadata> metadataList;

    @JsonProperty("customerSpace")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("extract_identifier")
    public String getExtractIdentifier() {
        return extractIdentifier;
    }

    @JsonProperty("extract_identifier")
    public void setExtractIdentifier(String extractIdentifier) {
        this.extractIdentifier = extractIdentifier;
    }

    @JsonProperty("data_category")
    public String getDataCategory() {
        return dataCategory;
    }

    @JsonProperty("data_category")
    public void setDataCategory(String dataCategory) {
        this.dataCategory = dataCategory;
    }

    @JsonProperty("table_name")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("table_name")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("vdb_query_handle")
    public String getVdbQueryHandle() {
        return vdbQueryHandle;
    }

    @JsonProperty("vdb_query_handle")
    public void setVdbQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    @JsonProperty("total_rows")
    public int getTotalRows() {
        return totalRows;
    }

    @JsonProperty("total_rows")
    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    @JsonProperty("get_query_data_endpoint")
    public String getGetQueryDataEndpoint() {
        return getQueryDataEndpoint;
    }

    @JsonProperty("get_query_data_endpoint")
    public void setGetQueryDataEndpoint(String getQueryDataEndpoint) {
        this.getQueryDataEndpoint = getQueryDataEndpoint;
    }

    @JsonProperty("report_status_endpoint")
    public String getReportStatusEndpoint() {
        return reportStatusEndpoint;
    }

    @JsonProperty("report_status_endpoint")
    public void setReportStatusEndpoint(String reportStatusEndpoint) {
        this.reportStatusEndpoint = reportStatusEndpoint;
    }

    @JsonProperty("vdb_spec_metadata")
    public List<VdbSpecMetadata> getMetadataList() {
        return metadataList;
    }

    @JsonProperty("vdb_spec_metadata")
    public void setMetadataList(List<VdbSpecMetadata> metadataList) {
        this.metadataList = metadataList;
    }

    @JsonProperty("merge_rule")
    public String getMergeRule() {
        return mergeRule;
    }

    @JsonProperty("merge_rule")
    public void setMergeRule(String mergeRule) {
        this.mergeRule = mergeRule;
    }

    @JsonProperty("create_table_rule")
    public VdbCreateTableRule getCreateTableRule() {
        return createTableRule;
    }

    @JsonProperty("create_table_rule")
    public void setCreateTableRule(VdbCreateTableRule createTableRule) {
        this.createTableRule = createTableRule;
    }
}
