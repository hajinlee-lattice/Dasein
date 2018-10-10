package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BusinessCalendarValidation {

    @JsonProperty("note")
    private String note;

    public BusinessCalendarValidation(String note) {
        this.note = note;
    }

    // for jackson
    @SuppressWarnings("unused")
	private BusinessCalendarValidation(){}

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }
}
