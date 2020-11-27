package com.latticeengines.domain.exposed.cdl.dashboard;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DashboardFilterValue implements Serializable {

    private static final long serialVersionUID = 0L;

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
        private final DashboardFilterValue dashboardFilterValue = new DashboardFilterValue();

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
