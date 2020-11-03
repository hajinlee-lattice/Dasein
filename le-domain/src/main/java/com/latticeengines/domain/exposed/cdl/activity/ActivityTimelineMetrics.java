package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.time.Period;

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

    private String METRICS_DESCRIPTION = "in last %d days";

    private static final String NEW_ACTIVITY_LABEL = "New Page Visits";
    private static final String NEW_IDENTIFIED_CONTACTS_LABEL = "Identified Contacts";
    private static final String NEW_ENGAGEMENTS_LABEL = "New Engagements";
    private static final String NEW_OPPORTUNITIES_LABEL = "New Opportunities";

    private static final String NEW_ACTIVITY_CONTEXT = "Total number of web activity in the last %d days";
    private static final String NEW_IDENTIFIED_CONTACTS_CONTEXT = "Total number of new identified contacts from Marketo in the last %d days";
    private static final String NEW_ENGAGEMENTS_CONTEXT = "Total number of engagements in the last in the last %d days";
    private static final String NEW_OPPORTUNITIES_CONTEXT = "Number of present open opportunities in the last %d days";

    public ActivityTimelineMetrics(Integer count, Period period, MetricsType type) {

        int days = period.getDays();

        this.count = count;
        this.description = String.format(METRICS_DESCRIPTION, days);

        switch (type) {
        case NewActivity:
            this.label = NEW_ACTIVITY_LABEL;
            this.context = String.format(NEW_ACTIVITY_CONTEXT, days);
            break;
        case NewIdentifiedContacts:
            this.label = NEW_IDENTIFIED_CONTACTS_LABEL;
            this.context = String.format(NEW_IDENTIFIED_CONTACTS_CONTEXT, days);
            break;
        case Newengagements:
            this.label = NEW_ENGAGEMENTS_LABEL;
            this.context = String.format(NEW_ENGAGEMENTS_CONTEXT, days);
            break;
        case NewOpportunities:
            this.label = NEW_OPPORTUNITIES_LABEL;
            this.context = String.format(NEW_OPPORTUNITIES_CONTEXT, days);
            break;
        }
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

    public enum MetricsType {
        NewActivity, NewIdentifiedContacts, Newengagements, NewOpportunities
    }
}
