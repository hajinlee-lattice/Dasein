package com.latticeengines.domain.exposed.cdl.dashboard;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DashboardResponse {
    @JsonProperty("dashboard_urls")
    private Map<String, String> dashboardUrls;

    @JsonProperty("filters")
    private Map<String, List<DashboardFilterValue>> filters;

    public Map<String, String> getDashboardUrls() {
        return dashboardUrls;
    }

    public void setDashboardUrls(Map<String, String> dashboardUrls) {
        this.dashboardUrls = dashboardUrls;
    }

    public Map<String, List<DashboardFilterValue>> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, List<DashboardFilterValue>> filters) {
        this.filters = filters;
    }
}
