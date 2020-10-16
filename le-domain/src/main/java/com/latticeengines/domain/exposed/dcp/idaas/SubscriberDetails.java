package com.latticeengines.domain.exposed.dcp.idaas;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubscriberDetails {

    /* This is an example of JSON returned from IDaaS

    {
    "subscriber_details": {
        "subscriber_number": "800118741",
        "duns_number": "167734092",
        "status": "Active",
        "company_name": "Test Acct 22",
        "effective_date": "2020-09-04",
        "expiration_date": "2021-09-03",
        "subscriber_type": "External",
        "service_type": "Paid Service",
        "address": {
            "iso2_country_code": "US"
        },
        "custom_param_1": null,
        "custom_param_2": null,
        "custom_param_3": null,
        "custom_param_4": null,
        "is_overage_allowed": null,
        "mail": "mikenesson@yopmail.com",
        "products": [
            "D+ Data Blocks"
        ]
    }
    }
     */

    @JsonProperty("subscriber_number")
    private String subscriberNumber;

    @JsonProperty("duns_number")
    private String dunsNumber;

    @JsonProperty("status")
    private String status;

    @JsonProperty("company_name")
    private String companyName;

    @JsonProperty("effective_date")  // format example 2020-09-04
    private Date effectiveDate;

    @JsonProperty("expiration_date")  // example 2020-09-04
    private Date expirationDate;

    @JsonProperty("subscriber_type")
    private String subscriberType;

    @JsonProperty("service_type")
    private String serviceType;

    @JsonProperty("address")
    private Address address;

    @JsonProperty("custom_param_1")
    private String customerParam1;

    @JsonProperty("custom_param_2")
    private String customerParam2;

    @JsonProperty("custom_param_3")
    private String customerParam3;

    @JsonProperty("custom_param_4")
    private String customerParam4;

    @JsonProperty("is_overage_allowed")
    private String isOverageAllowed;

    @JsonProperty("mail")
    private String mail;

    @JsonProperty("products")
    private List<String> products;

    public static class Address {

        @JsonProperty("iso2_country_code")
        private String countryCode;

        @JsonProperty("street")
        private String street;

        @JsonProperty("street2")
        private String street2;

        @JsonProperty("street3")
        private String street3;

        @JsonProperty("city")
        private String city;

        @JsonProperty("state")
        private String state;

        @JsonProperty("postal_code")
        private String postalCode;

        public String getCountryCode() {
            return countryCode;
        }

        public void setCountryCode(String countryCode) {
            this.countryCode = countryCode;
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getStreet2() {
            return street2;
        }

        public void setStreet2(String street2) {
            this.street2 = street2;
        }

        public String getStreet3() {
            return street3;
        }

        public void setStreet3(String street3) {
            this.street3 = street3;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getPostalCode() {
            return postalCode;
        }

        public void setPostalCode(String postalCode) {
            this.postalCode = postalCode;
        }
    }

    public String getSubscriberNumber() {
        return subscriberNumber;
    }

    public void setSubscriberNumber(String subscriberNumber) {
        this.subscriberNumber = subscriberNumber;
    }

    public String getDunsNumber() {
        return dunsNumber;
    }

    public void setDunsNumber(String dunsNumber) {
        this.dunsNumber = dunsNumber;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public Date getEffectiveDate() {
        return effectiveDate;
    }

    public void setEffectiveDate(Date effectiveDate) {
        this.effectiveDate = effectiveDate;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    public String getSubscriberType() {
        return subscriberType;
    }

    public void setSubscriberType(String subscriberType) {
        this.subscriberType = subscriberType;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public String getCustomerParam1() {
        return customerParam1;
    }

    public void setCustomerParam1(String customerParam1) {
        this.customerParam1 = customerParam1;
    }

    public String getCustomerParam2() {
        return customerParam2;
    }

    public void setCustomerParam2(String customerParam2) {
        this.customerParam2 = customerParam2;
    }

    public String getCustomerParam3() {
        return customerParam3;
    }

    public void setCustomerParam3(String customerParam3) {
        this.customerParam3 = customerParam3;
    }

    public String getCustomerParam4() {
        return customerParam4;
    }

    public void setCustomerParam4(String customerParam4) {
        this.customerParam4 = customerParam4;
    }

    public String getOverageAllowed() {
        return isOverageAllowed;
    }

    public void setOverageAllowed(String overageAllowed) {
        isOverageAllowed = overageAllowed;
    }

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    public List<String> getProducts() {
        return products;
    }

    public void setProducts(List<String> products) {
        this.products = products;
    }
}
