package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VdbConnectorConfiguration extends ConnectorConfiguration {

    @JsonProperty("dl_data_ready")
    private boolean dlDataReady;

    @JsonProperty("dl_load_group")
    private String dlLoadGroup;

    @JsonProperty("dl_tenant_id")
    private String dlTenantId;

    @JsonProperty("dl_endpoint")
    private String dlEndpoint;

    @JsonProperty("get_query_data_endpoint")
    private String getQueryDataEndpoint;

    @JsonProperty("report_status_endpoint")
    private String reportStatusEndpoint;

    @JsonProperty("table_configurations")
    private LinkedHashMap<String, ImportVdbTableConfiguration> tableConfigurations = new LinkedHashMap<>();

    public boolean isDlDataReady() {
        return dlDataReady;
    }

    public void setDlDataReady(boolean dlDataReady) {
        this.dlDataReady = dlDataReady;
    }

    public String getDlLoadGroup() {
        return dlLoadGroup;
    }

    public void setDlLoadGroup(String dlLoadGroup) {
        this.dlLoadGroup = dlLoadGroup;
    }

    public String getDlTenantId() {
        return dlTenantId;
    }

    public void setDlTenantId(String dlTenantId) {
        this.dlTenantId = dlTenantId;
    }

    public String getDlEndpoint() {
        return dlEndpoint;
    }

    public void setDlEndpoint(String dlEndpoint) {
        this.dlEndpoint = dlEndpoint;
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

    public LinkedHashMap<String, ImportVdbTableConfiguration> getTableConfigurations() {
        return tableConfigurations;
    }

    public void setTableConfigurations(LinkedHashMap<String, ImportVdbTableConfiguration> tableConfigurations) {
        this.tableConfigurations = tableConfigurations;
    }

    public void addTableConfiguration(String tableName, ImportVdbTableConfiguration tableConfiguration) {
        tableConfigurations.put(tableName, tableConfiguration);
    }

    public ImportVdbTableConfiguration getVdbTableConfiguration(String tableName) {
        ImportVdbTableConfiguration configuration = null;
        if (tableConfigurations.containsKey(tableName)) {
            configuration = tableConfigurations.get(tableName);
        }
        return configuration;
    }
}
