package com.latticeengines.domain.exposed.dcp.vbo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

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
        @JsonProperty("orderID")
        private String orderId;

        @JsonProperty("participatingPointID")
        private String participatingPointId;

        @JsonProperty("orderNumber")
        private String orderNumber;

        @JsonProperty("orderLineItemID")
        private String orderLineItemId;

        @JsonProperty("assetLineItemID")
        private String assetLineItemId;

        @JsonProperty("assetLineItemName")
        private String assetLineItemName;

        @JsonProperty("orderLineItemName")
        private String orderLineItemName;

        @JsonProperty("agreementID")
        private String agreementId;

        @JsonProperty("agreementNumber")
        private String agreementNumber;

        @JsonProperty("referenceID")
        private String referenceId;

        @JsonProperty("OverageAssetLineItemID1")
        private String OverageAssetLineItemId1;

        @JsonProperty("OverageAssetLineItemID2")
        private String OverageAssetLineItemId2;

        @JsonProperty("OverageAssetLineItemID3")
        private String OverageAssetLineItemId3;

        @JsonProperty("OvereageAssetlineItemName1")
        private String OvereageAssetlineItemName1;

        @JsonProperty("OvereageAssetlineItemName2")
        private String OvereageAssetlineItemName2;

        @JsonProperty("OvereageAssetlineItemName3")
        private String OvereageAssetlineItemName3;

        @JsonProperty("users")
        private List<User> users;

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getParticipatingPointId() {
            return participatingPointId;
        }

        public void setParticipatingPointId(String participatingPointId) {
            this.participatingPointId = participatingPointId;
        }

        public String getOrderNumber() {
            return orderNumber;
        }

        public void setOrderNumber(String orderNumber) {
            this.orderNumber = orderNumber;
        }

        public String getOrderLineItemId() {
            return orderLineItemId;
        }

        public void setOrderLineItemId(String orderLineItemId) {
            this.orderLineItemId = orderLineItemId;
        }

        public String getAssetLineItemId() {
            return assetLineItemId;
        }

        public void setAssetLineItemId(String assetLineItemId) {
            this.assetLineItemId = assetLineItemId;
        }

        public String getAssetLineItemName() {
            return assetLineItemName;
        }

        public void setAssetLineItemName(String assetLineItemName) {
            this.assetLineItemName = assetLineItemName;
        }

        public String getOrderLineItemName() {
            return orderLineItemName;
        }

        public void setOrderLineItemName(String orderLineItemName) {
            this.orderLineItemName = orderLineItemName;
        }

        public String getAgreementId() {
            return agreementId;
        }

        public void setAgreementId(String agreementId) {
            this.agreementId = agreementId;
        }

        public String getAgreementNumber() {
            return agreementNumber;
        }

        public void setAgreementNumber(String agreementNumber) {
            this.agreementNumber = agreementNumber;
        }

        public String getReferenceId() {
            return referenceId;
        }

        public void setReferenceId(String referenceId) {
            this.referenceId = referenceId;
        }

        public String getOverageAssetLineItemId1() {
            return OverageAssetLineItemId1;
        }

        public void setOverageAssetLineItemId1(String overageAssetLineItemId1) {
            OverageAssetLineItemId1 = overageAssetLineItemId1;
        }

        public String getOverageAssetLineItemId2() {
            return OverageAssetLineItemId2;
        }

        public void setOverageAssetLineItemId2(String overageAssetLineItemId2) {
            OverageAssetLineItemId2 = overageAssetLineItemId2;
        }

        public String getOverageAssetLineItemId3() {
            return OverageAssetLineItemId3;
        }

        public void setOverageAssetLineItemId3(String overageAssetLineItemId3) {
            OverageAssetLineItemId3 = overageAssetLineItemId3;
        }

        public String getOvereageAssetlineItemName1() {
            return OvereageAssetlineItemName1;
        }

        public void setOvereageAssetlineItemName1(String overeageAssetlineItemName1) {
            OvereageAssetlineItemName1 = overeageAssetlineItemName1;
        }

        public String getOvereageAssetlineItemName2() {
            return OvereageAssetlineItemName2;
        }

        public void setOvereageAssetlineItemName2(String overeageAssetlineItemName2) {
            OvereageAssetlineItemName2 = overeageAssetlineItemName2;
        }

        public String getOvereageAssetlineItemName3() {
            return OvereageAssetlineItemName3;
        }

        public void setOvereageAssetlineItemName3(String overeageAssetlineItemName3) {
            OvereageAssetlineItemName3 = overeageAssetlineItemName3;
        }

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
