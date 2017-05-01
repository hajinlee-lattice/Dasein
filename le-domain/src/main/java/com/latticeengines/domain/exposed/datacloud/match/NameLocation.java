package com.latticeengines.domain.exposed.datacloud.match;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NameLocation implements Fact, Serializable {

    private static final long serialVersionUID = 8509368904723299727L;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Country")
    private String country;

    @JsonProperty("CountryCode")
    private String countryCode;

    @JsonProperty("State")
    private String state;

    @JsonProperty("City")
    private String city;

    @JsonProperty("Street")
    private String street;

    @JsonProperty("Zipcode")
    private String zipcode;

    @JsonProperty("PhoneNumber")
    private String phoneNumber;

    public static NameLocation fromMatchKeyTuple(MatchKeyTuple keyTuple) {
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(keyTuple.getName());
        nameLocation.setCity(keyTuple.getCity());
        nameLocation.setState(keyTuple.getState());
        nameLocation.setCountry(keyTuple.getCountry());
        nameLocation.setZipcode(keyTuple.getZipcode());
        nameLocation.setPhoneNumber(keyTuple.getPhoneNumber());
        return nameLocation;
    }

    @MetricField(name = "Name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @MetricField(name = "Country")
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @MetricField(name = "CountryCode")
    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    @MetricField(name = "State")
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @MetricField(name = "City")
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    @MetricField(name = "Zipcode")
    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    @MetricField(name = "PhoneNumber")
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
        if (that instanceof NameLocation) {
            NameLocation nameLocation = (NameLocation) that;
            return StringUtils.equals(this.name, nameLocation.name)
                    && StringUtils.equals(this.city, nameLocation.city)
                    && StringUtils.equals(this.state, nameLocation.state)
                    && StringUtils.equals(this.zipcode, nameLocation.zipcode)
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
        toReturn += (zipcode == null ? "null" : zipcode);
        toReturn += (country == null ? "null" : country);
        toReturn += (phoneNumber == null ? "null" : phoneNumber);
        toReturn += ")";
        return toReturn;
    }

}
