package com.latticeengines.domain.exposed.cdl.dashboard;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DashboardFilterValue {

    @JsonProperty("display_name")
    private String displayName;
    @JsonProperty("value")
    private String value;

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static final class Builder {
        private DashboardFilterValue dashboardFilterValue;

        public Builder withDisplayName(String displayName) {
            dashboardFilterValue.setDisplayName(displayName);
            return this;
        }

        public Builder withValue(String value) {
            dashboardFilterValue.setValue(value);
            return this;
        }

        public DashboardFilterValue build() {
            return this.dashboardFilterValue;
        }
    }
}
