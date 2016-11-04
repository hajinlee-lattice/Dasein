package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.datacloud.match.service.impl.MatchConstants;

public class MatchKeyTuple implements Fact {
    private String domain;
    private String name;
    private String city;
    private String state;
    private String country;
    private String countryCode;
    private String zipcode;
    private String phoneNumber;
    private String duns;
    private String email;

    @MetricField(name = MatchConstants.DOMAIN_FIELD)
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @MetricField(name = MatchConstants.NAME_FIELD)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @MetricField(name = MatchConstants.CITY_FIELD)
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @MetricField(name = MatchConstants.STATE_FIELD)
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @MetricField(name = MatchConstants.COUNTRY_FIELD)
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

    @MetricField(name = MatchConstants.ZIPCODE_FIELD)
    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    @MetricField(name = MatchConstants.PHONE_NUM_FIELD)
    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @MetricField(name = MatchConstants.DUNS_FIELD)
    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    @MetricField(name = MatchConstants.EMAIL_FIELD)
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
    
    
}
