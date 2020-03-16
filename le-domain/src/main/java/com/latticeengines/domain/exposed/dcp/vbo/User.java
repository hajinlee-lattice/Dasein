package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class User {
    @JsonProperty("name")
    private Name name;

    @JsonProperty("primaryAddress")
    private PrimaryAddress primaryAddress;

    @JsonProperty("userId")
    private String userId;

    @JsonProperty("title")
    private String title;

    @JsonProperty("emailAddress")
    private String emailAddress;

    @JsonProperty("telephoneNumber")
    private String telephoneNumber;

    private class Name {
        @JsonProperty("firstName")
        private String firstName;

        @JsonProperty("lastName")
        private String lastName;
    }

    private class PrimaryAddress {
        @JsonProperty("streetAddressLine1")
        private String streetAddressLine1;

        @JsonProperty("primaryTownName")
        private String primaryTownName;

        @JsonProperty("territoryName")
        private String territoryName;

        @JsonProperty("postalCode")
        private String postalCode;

        @JsonProperty("countryISOAlpha2Code")
        private String countryISOAlpha2Code;
    }

    public Name getName() {
        return name;
    }

    public void setName(Name name) {
        this.name = name;
    }

    public PrimaryAddress getPrimaryAddress() {
        return primaryAddress;
    }

    public void setPrimaryAddress(PrimaryAddress primaryAddress) {
        this.primaryAddress = primaryAddress;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getTelephoneNumber() {
        return telephoneNumber;
    }

    public void setTelephoneNumber(String telephoneNumber) {
        this.telephoneNumber = telephoneNumber;
    }
}
