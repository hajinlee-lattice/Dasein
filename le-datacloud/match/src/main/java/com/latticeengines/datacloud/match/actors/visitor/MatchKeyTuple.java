package com.latticeengines.datacloud.match.actors.visitor;

import org.apache.commons.lang.StringUtils;

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

    private String serializedFormat;

    @MetricField(name = MatchConstants.DOMAIN_FIELD)
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.NAME_FIELD)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.CITY_FIELD)
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.STATE_FIELD)
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.COUNTRY_FIELD)
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
        constructSerializedFormat();
    }

    @MetricField(name = "CountryCode")
    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.ZIPCODE_FIELD)
    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.PHONE_NUM_FIELD)
    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.DUNS_FIELD)
    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
        constructSerializedFormat();
    }

    @MetricField(name = MatchConstants.EMAIL_FIELD)
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
        constructSerializedFormat();
    }

    @Override
    public String toString() {
        return serializedFormat;
    }

    private void constructSerializedFormat() {
        StringBuilder sb = new StringBuilder("( ");
        if (StringUtils.isNotEmpty(duns)) {
            sb.append(String.format("%s=%s ", MatchConstants.DUNS_FIELD, duns));
        }
        if (StringUtils.isNotEmpty(domain)) {
            sb.append(String.format("%s=%s ", MatchConstants.DOMAIN_FIELD, domain));
        }
        if (StringUtils.isNotEmpty(name)) {
            sb.append(String.format("%s=\"%s\" ", MatchConstants.NAME_FIELD, name));
        }
        if (StringUtils.isNotEmpty(city)) {
            sb.append(String.format("%s=\"%s\" ", MatchConstants.CITY_FIELD, city));
        }
        if (StringUtils.isNotEmpty(state)) {
            sb.append(String.format("%s=\"%s\" ", MatchConstants.STATE_FIELD, state));
        }
        if (StringUtils.isNotEmpty(zipcode)) {
            sb.append(String.format("%s=%s ", MatchConstants.ZIPCODE_FIELD, zipcode));
        }
        if (StringUtils.isNotEmpty(country)) {
            sb.append(String.format("%s=\"%s\" ", MatchConstants.COUNTRY_FIELD, country));
        }
        if (StringUtils.isNotEmpty(countryCode)) {
            sb.append(String.format("%s=%s ", MatchConstants.COUNTRY_CODE_FIELD, countryCode));
        }
        if (StringUtils.isNotEmpty(phoneNumber)) {
            sb.append(String.format("%s=%s ", MatchConstants.PHONE_NUM_FIELD, phoneNumber));
        }
        if (StringUtils.isNotEmpty(email)) {
            sb.append(String.format("%s=%s ", MatchConstants.EMAIL_FIELD, email));
        }
        sb.append(")");
        serializedFormat = sb.toString();
    }
}
