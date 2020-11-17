package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityTimelineMetrics implements Serializable {

    @JsonProperty("count")
    private Integer count;

    @JsonProperty("label")
    private String label;

    @JsonProperty("description")
    private String description;

    @JsonProperty("context")
    private String context;

    public ActivityTimelineMetrics() {

    }

    public ActivityTimelineMetrics(Integer count, String lable, String description, String context) {

        this.count = count;
        this.label = lable;
        this.description = description;
        this.context = context;
    }

    public Integer getCount() {
        return count;
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

    public void setCount(Integer count) {
        this.count = count;
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
            public String getContext(Integer count) {
                return String.format("Total number of web activity in the last %d days", count);
            }
        },
        NewContacts("New Contacts") {
            @Override
            public String getContext(Integer count) {
                return String.format("Total number of new contacts in the last %d days", count);
            }
        },
        Newengagements("New Engagements") {
            @Override
            public String getContext(Integer count) {
                return String.format("Total number of engagements in the last in the last %d days", count);
            }
        },
        NewOpportunities("New Opportunities") {
            @Override
            public String getContext(Integer count) {
                return String.format("Number of present open opportunities in the last %d days", count);
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

            if (count.equals(0)) {
                return String.valueOf(count);
            }
            return String.format("%d in last %d days", count, days);
        }

        public abstract String getContext(Integer count);
    }
}
