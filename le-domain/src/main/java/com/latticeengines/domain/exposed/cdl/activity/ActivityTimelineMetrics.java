package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityTimelineMetrics implements Serializable {

    @JsonProperty("message")
    private String message;

    @JsonProperty("label")
    private String label;

    @JsonProperty("description")
    private String description;

    @JsonProperty("context")
    private String context;

    public ActivityTimelineMetrics() {

    }

    public ActivityTimelineMetrics(String message, String lable, String description, String context) {

        this.message = message;
        this.label = lable;
        this.description = description;
        this.context = context;
    }

    public String getMessage() {
        return message;
    }

    public String getLabel() {
        return label;
    }

    public String getDescription() {
        return description;
    }

    public String getContext() {
        return context;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public enum MetricsType {

        NewActivities("New Activities") {
            @Override
            public String getContext(Integer days) {
                return String.format("Total number of web activity in the last %d days", days);
            }
        },
        NewContacts("New Contacts") {
            @Override
            public String getContext(Integer days) {
                return String.format("Total number of new contacts in the last %d days", days);
            }
        },
        Newengagements("New Engagements") {
            @Override
            public String getContext(Integer days) {
                return String.format("Total number of engagements in the last %d days", days);
            }
        },
        NewOpportunities("New Opportunities") {
            @Override
            public String getContext(Integer days) {
                return String.format("Number of present open opportunities in the last %d days", days);
            }
        },
        AccountIntent("Account Intent") {
            @Override
            public String getContext(Integer days) {
                return "Acount-level intent in the last 10 days";
            }
        };

        private String label;

        MetricsType(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }

        public static String getDescription(Integer count, Integer days) {
            if (count == null || count.equals(0)) {
                return "";
            }
            return String.format(" in last %d days", days);
        }

        public abstract String getContext(Integer days);
    }
}
