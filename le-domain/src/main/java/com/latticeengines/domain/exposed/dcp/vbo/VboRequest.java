package com.latticeengines.domain.exposed.dcp.vbo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class VboRequest {

    @JsonProperty("subscriber")
    @ApiModelProperty(required = true, value = "subscriber")
    private Subscriber subscriber;

    @JsonProperty("product")
    @ApiModelProperty(required = true, value = "product")
    private Product product;

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public void setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }


    public static class Name {
        @JsonProperty("firstName")
        private String firstName;

        @JsonProperty("lastName")
        private String lastName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
    }

    public static class PrimaryAddress {
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

        public String getStreetAddressLine1() {
            return streetAddressLine1;
        }

        public void setStreetAddressLine1(String streetAddressLine1) {
            this.streetAddressLine1 = streetAddressLine1;
        }

        public String getPrimaryTownName() {
            return primaryTownName;
        }

        public void setPrimaryTownName(String primaryTownName) {
            this.primaryTownName = primaryTownName;
        }

        public String getTerritoryName() {
            return territoryName;
        }

        public void setTerritoryName(String territoryName) {
            this.territoryName = territoryName;
        }

        public String getPostalCode() {
            return postalCode;
        }

        public void setPostalCode(String postalCode) {
            this.postalCode = postalCode;
        }

        public String getCountryISOAlpha2Code() {
            return countryISOAlpha2Code;
        }

        public void setCountryISOAlpha2Code(String countryISOAlpha2Code) {
            this.countryISOAlpha2Code = countryISOAlpha2Code;
        }
    }

    public static class Product {

        @JsonProperty("users")
        private List<User> users;

        public List<User> getUsers() {
            return users;
        }

        public void setUsers(List<User> users) {
            this.users = users;
        }

    }

    public static class Subscriber {

        @JsonProperty("subscriberNumber")
        private String subscriberNumber;

        @JsonProperty("language")
        private String language;

        @JsonProperty("name")
        private String name;

        public String getSubscriberNumber() {
            return subscriberNumber;
        }

        public void setSubscriberNumber(String subscriberNumber) {
            this.subscriberNumber = subscriberNumber;
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
    }

    public static class User {
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
}
