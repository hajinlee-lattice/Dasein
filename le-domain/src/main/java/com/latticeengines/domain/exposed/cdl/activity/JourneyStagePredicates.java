package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JourneyStagePredicates implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("stream_type")
    private AtlasStream.StreamType streamType;

    //the unit here is day and evaluating from current time.
    @JsonProperty("period")
    private int period;

    @JsonProperty("stream_field_to_filter_list")
    private List<StreamFieldToFilter> streamFieldToFilterList;

    @JsonProperty("no_of_events")
    private int noOfEvents;

    @JsonProperty("contact_not_null")
    private boolean contactNotNull;

    public AtlasStream.StreamType getStreamType() {
        return streamType;
    }

    public void setStreamType(AtlasStream.StreamType streamType) {
        this.streamType = streamType;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public List<StreamFieldToFilter> getStreamFieldToFilterList() {
        return streamFieldToFilterList;
    }

    public void setStreamFieldToFilterList(List<StreamFieldToFilter> streamFieldToFilterList) {
        this.streamFieldToFilterList = streamFieldToFilterList;
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
