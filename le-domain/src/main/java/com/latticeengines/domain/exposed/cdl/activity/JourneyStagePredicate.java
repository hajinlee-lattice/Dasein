package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JourneyStagePredicate implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("stream_type")
    private AtlasStream.StreamType streamType;

    // the unit here is day and evaluating from current time.
    @JsonProperty("period_days")
    private int periodDays;

    @JsonProperty("stream_fields_to_filter")
    private List<StreamFieldToFilter> streamFieldsToFilter;

    @JsonProperty("no_of_events")
    private int noOfEvents;

    @JsonProperty("contact_not_null")
    private boolean contactNotNull;

    public AtlasStream.StreamType getStreamType() { return streamType; }

    public void setStreamType(AtlasStream.StreamType streamType) {
        this.streamType = streamType;
    }

    public int getPeriodDays() { return periodDays; }

    public void setPeriodDays(int periodDays) { this.periodDays = periodDays; }

    public List<StreamFieldToFilter> getStreamFieldsToFilter() { return streamFieldsToFilter; }

    public void setStreamFieldsToFilter(List<StreamFieldToFilter> streamFieldsToFilter) {
        this.streamFieldsToFilter = streamFieldsToFilter;
    }

    public int getNoOfEvents() {
        return noOfEvents;
    }

    public void setNoOfEvents(int noOfEvents) {
        this.noOfEvents = noOfEvents;
    }

    public boolean isContactNotNull() {
        return contactNotNull;
    }

    public void setContactNotNull(boolean contactNotNull) {
        this.contactNotNull = contactNotNull;
    }
}
