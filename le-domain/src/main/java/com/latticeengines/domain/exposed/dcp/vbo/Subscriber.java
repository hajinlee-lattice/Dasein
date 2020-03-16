package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Subscriber {

    @JsonProperty("subscriberNumber")
    private String subscriberNumber;

    @JsonProperty("countryISOAlpha2Code")
    private String countryISOAlpha2Code;

    @JsonProperty("duns")
    private String duns;

    @JsonProperty("language")
    private String language;

    @JsonProperty("name")
    private String name;

    @JsonProperty("accountID")
    private String accountId;

    @JsonProperty("subscriberType")
    private String subscriberType;

    public String getSubscriberNumber() {
        return subscriberNumber;
    }

    public void setSubscriberNumber(String subscriberNumber) {
        this.subscriberNumber = subscriberNumber;
    }

    public String getCountryISOAlpha2Code() {
        return countryISOAlpha2Code;
    }

    public void setCountryISOAlpha2Code(String countryISOAlpha2Code) {
        this.countryISOAlpha2Code = countryISOAlpha2Code;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getSubscriberType() {
        return subscriberType;
    }

    public void setSubscriberType(String subscriberType) {
        this.subscriberType = subscriberType;
    }

}

