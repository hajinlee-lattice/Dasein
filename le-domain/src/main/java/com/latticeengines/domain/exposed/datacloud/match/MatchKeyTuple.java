package com.latticeengines.domain.exposed.datacloud.match;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchKeyTuple implements Fact {

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("City")
    private String city;

    @JsonProperty("State")
    private String state;

    @JsonProperty("Country")
    private String country;

    @JsonIgnore
    private String countryCode;

    @JsonProperty("ZipCode")
    private String zipcode;

    @JsonProperty("PhoneNumber")
    private String phoneNumber;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("Email")
    private String email;

    @JsonIgnore
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

    public boolean hasDomain() {
        return StringUtils.isNotEmpty(domain);
    }

    public boolean hasName() {
        return StringUtils.isNotEmpty(name);
    }

    public boolean hasCity() {
        return StringUtils.isNotEmpty(city);
    }

    public boolean hasState() {
        return StringUtils.isNotEmpty(state);
    }

    public boolean hasCountry() {
        return StringUtils.isNotEmpty(country);
    }

    public boolean hasZipCode() {
        return StringUtils.isNotEmpty(zipcode);
    }

    public boolean hasLocation() {
        return hasCity() || hasState() || hasCountry() || hasZipCode();
    }

    @Override
    public String toString() {
        return serializedFormat;
    }

    private void constructSerializedFormat() {
        StringBuilder sb = new StringBuilder("( ");
        if (StringUtils.isNotEmpty(duns)) {
            sb.append(String.format("%s=%s, ", MatchConstants.DUNS_FIELD, duns));
        }
        if (StringUtils.isNotEmpty(domain)) {
            sb.append(String.format("%s=%s, ", MatchConstants.DOMAIN_FIELD, domain));
        }
        if (StringUtils.isNotEmpty(name)) {
            sb.append(String.format("%s=%s, ", MatchConstants.NAME_FIELD, name));
        }
        if (StringUtils.isNotEmpty(city)) {
            sb.append(String.format("%s=%s, ", MatchConstants.CITY_FIELD, city));
        }
        if (StringUtils.isNotEmpty(state)) {
            sb.append(String.format("%s=%s, ", MatchConstants.STATE_FIELD, state));
        }
        if (StringUtils.isNotEmpty(zipcode)) {
            sb.append(String.format("%s=%s, ", MatchConstants.ZIPCODE_FIELD, zipcode));
        }
        if (StringUtils.isNotEmpty(country)) {
            sb.append(String.format("%s=%s, ", MatchConstants.COUNTRY_FIELD, country));
        }
        if (StringUtils.isNotEmpty(countryCode)) {
            sb.append(String.format("%s=%s, ", MatchConstants.COUNTRY_CODE_FIELD, countryCode));
        }
        if (StringUtils.isNotEmpty(phoneNumber)) {
            sb.append(String.format("%s=%s, ", MatchConstants.PHONE_NUM_FIELD, phoneNumber));
        }
        if (StringUtils.isNotEmpty(email)) {
            sb.append(String.format("%s=%s, ", MatchConstants.EMAIL_FIELD, email));
        }
        sb.append(")");
        serializedFormat = sb.toString();
    }
}
