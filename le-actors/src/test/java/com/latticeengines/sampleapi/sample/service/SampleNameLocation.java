package com.latticeengines.sampleapi.sample.service;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SampleNameLocation {

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Country")
    private String country;

    @JsonProperty("State")
    private String state;

    @JsonProperty("City")
    private String city;

    @JsonProperty("ZipCode")
    private String zipCode;

    @JsonProperty("PhoneNumber")
    private String phoneNumber;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof SampleNameLocation) {
            SampleNameLocation nameLocation = (SampleNameLocation) that;
            return StringUtils.equals(this.name, nameLocation.name) //
                    && StringUtils.equals(this.city, nameLocation.city)
                    && StringUtils.equals(this.state, nameLocation.state)
                    && StringUtils.equals(this.zipCode, nameLocation.zipCode)
                    && StringUtils.equals(this.country, nameLocation.country)
                    && StringUtils.equals(this.phoneNumber, nameLocation.phoneNumber);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String toReturn = "(";
        toReturn += (name == null ? "null" : name);
        toReturn += (city == null ? "null" : city);
        toReturn += (state == null ? "null" : state);
        toReturn += (zipCode == null ? "null" : zipCode);
        toReturn += (country == null ? "null" : country);
        toReturn += (phoneNumber == null ? "null" : phoneNumber);
        toReturn += ")";
        return toReturn;
    }

}
