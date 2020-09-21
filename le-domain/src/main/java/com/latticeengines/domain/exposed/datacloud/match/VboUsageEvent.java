package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class VboUsageEvent {

    @JsonProperty("poaeId")
    private String poaeId;

    @JsonProperty("eventTime")
    private String eventTime;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("featureUri")
    private String featureUri;

    @JsonProperty("subjectDuns")
    private String subjectDuns;

    @JsonProperty("subjectName")
    private String subjectName;

    @JsonProperty("subjectCity")
    private String subjectCity;

    @JsonProperty("subjectState")
    private String subjectState;

    @JsonProperty("subjectCountry")
    private String subjectCountry;

    @JsonProperty("responseTime")
    private Long responseTime;

    public String getPoaeId() {
        return poaeId;
    }

    public void setPoaeId(String poaeId) {
        this.poaeId = poaeId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getFeatureUri() {
        return featureUri;
    }

    public void setFeatureUri(String featureUri) {
        this.featureUri = featureUri;
    }

    public String getSubjectDuns() {
        return subjectDuns;
    }

    public void setSubjectDuns(String subjectDuns) {
        this.subjectDuns = subjectDuns;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getSubjectCity() {
        return subjectCity;
    }

    public void setSubjectCity(String subjectCity) {
        this.subjectCity = subjectCity;
    }

    public String getSubjectState() {
        return subjectState;
    }

    public void setSubjectState(String subjectState) {
        this.subjectState = subjectState;
    }

    public String getSubjectCountry() {
        return subjectCountry;
    }

    public void setSubjectCountry(String subjectCountry) {
        this.subjectCountry = subjectCountry;
    }

    public Long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(Long responseTime) {
        this.responseTime = responseTime;
    }
}
