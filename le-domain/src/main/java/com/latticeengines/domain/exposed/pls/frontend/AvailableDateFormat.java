package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AvailableDateFormat {

    @JsonProperty("date_format")
    private List<String> dateFormats;

    @JsonProperty("time_format")
    private List<String> timeFormats;

    @JsonProperty("timezones")
    private List<String> timezones;

    public List<String> getDateFormats() {
        return dateFormats;
    }

    public void setDateFormats(List<String> dateFormats) {
        this.dateFormats = dateFormats;
    }

    public List<String> getTimeFormats() {
        return timeFormats;
    }

    public void setTimeFormats(List<String> timeFormats) {
        this.timeFormats = timeFormats;
    }

    public List<String> getTimezones() {
        return timezones;
    }

    public void setTimezones(List<String> timezones) {
        this.timezones = timezones;
    }
}
