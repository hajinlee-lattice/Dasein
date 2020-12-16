package com.latticeengines.domain.exposed.dcp.vbo;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude
public class VboUserSeatUsageEvent {

    public enum FeatureURI { STCT, STCTDEC }
    @JsonProperty
    private final Object GUID = null;
    @JsonProperty
    private final Object agentID = null;
    @JsonProperty
    private final Object capID = null;
    @JsonProperty
    private final Object consumerIP = null;
    @JsonProperty
    private final Object subjectDunsEntityId = null;
    @JsonProperty
    private final Object reasonCode = null;
    @JsonProperty
    private final Object customerReference = null;
    @JsonProperty
    private final Object userLocation = null;
    @JsonProperty
    private final Object subjectName = null;
    @JsonProperty
    private final Object subjectCity = null;
    @JsonProperty
    private final Object subjectState = null;
    @JsonProperty
    private final Object orderedBy = null;

    @JsonProperty
    private final String drt = "D&B Connect";
    @JsonProperty
    private final String eventType = "Report";
    @JsonProperty
    private final String deliveryChannel = "Web Application";

    @JsonProperty
    private final Integer appID = 157;
    @JsonProperty
    private final Integer responseTime = 0;
    @JsonProperty
    private final Integer portfolioSize = 1;

    @JsonProperty
    private String emailAddress;

    @JsonProperty
    private Long LUID;

    @JsonProperty
    private String POAEID;

    @JsonProperty
    private String subscriberID;

    @JsonProperty
    private String timeStamp;

    @JsonProperty
    private FeatureURI featureURI;

    @JsonProperty
    private String subscriberCountry;
    @JsonProperty
    private String subjectCountry;
    @JsonProperty
    private Date contractTermStartDate;
    @JsonProperty
    private Date contractTermEndDate;

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }
    public void setLUID(Long LUID) {
        this.LUID = LUID;
    }
    public void setPOAEID(String POAEID) {
        this.POAEID = POAEID;
    }
    public void setSubscriberID(String subscriberID) {
        this.subscriberID = subscriberID;
    }
    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }
    public void setFeatureURI(FeatureURI featureURI) {
        this.featureURI = featureURI;
    }
    public void setSubscriberCountry(String subscriberCountry) {
        this.subscriberCountry = subscriberCountry;
    }
    public void setSubjectCountry(String subjectCountry) {
        this.subjectCountry = subjectCountry;
    }
    public void setContractTermStartDate(Date contractTermStartDate) {
        this.contractTermStartDate = contractTermStartDate;
    }
    public void setContractTermEndDate(Date contractTermEndDate) {
        this.contractTermEndDate = contractTermEndDate;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public String getSubscriberID() {
        return subscriberID;
    }
}
