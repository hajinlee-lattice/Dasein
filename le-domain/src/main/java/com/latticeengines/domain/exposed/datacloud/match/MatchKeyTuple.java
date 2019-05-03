package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchKeyTuple implements Fact {

    private static final Logger log = LoggerFactory.getLogger(MatchKeyTuple.class);

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

    // A list of pairs of System Id name and value.
    @JsonProperty("SystemIds")
    private List<Pair<String, String>> systemIds;

    @JsonIgnore
    private String serializedFormat;

    @JsonIgnore
    private String uniqueIdForKey;

    @JsonIgnore
    private String uniqueIdForValue;

    private boolean domainFromMultiCandidates;

    @MetricField(name = MatchConstants.DOMAIN_FIELD)
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.NAME_FIELD)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.CITY_FIELD)
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.STATE_FIELD)
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.COUNTRY_FIELD)
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
        refreshCachedStrings();
    }

    @MetricField(name = "CountryCode")
    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
        // NOTE not actually required, just keep consistent in case unique ID
        // implementation changed
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.ZIPCODE_FIELD)
    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.PHONE_NUM_FIELD)
    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.DUNS_FIELD)
    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
        refreshCachedStrings();
    }

    @MetricField(name = MatchConstants.EMAIL_FIELD)
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
        refreshCachedStrings();
    }

    public List<Pair<String, String>> getSystemIds() {
        return systemIds;
    }

    public void setSystemIds(List<Pair<String, String>> systemIds) {
        this.systemIds = systemIds;
        refreshCachedStrings();
    }

    public boolean isDomainFromMultiCandidates() {
        return domainFromMultiCandidates;
    }

    public void setDomainFromMultiCandidates(boolean domainFromMultiCandidates) {
        this.domainFromMultiCandidates = domainFromMultiCandidates;
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

    public boolean hasPhoneNumber() {
        return StringUtils.isNotEmpty(phoneNumber);
    }

    public boolean hasDuns() {
        return StringUtils.isNotEmpty(duns);
    }

    public boolean hasEmail() {
        return StringUtils.isNotEmpty(email);
    }

    public boolean hasSystemIds() {
        return CollectionUtils.isNotEmpty(systemIds)
                && systemIds.stream().anyMatch(pair -> StringUtils.isNotEmpty(pair.getRight()));
    }

    public boolean hasDomainOnly() {
        return hasDomain()
                && !(hasName() || hasLocation() || hasPhoneNumber() || hasDuns() || hasEmail() || hasSystemIds());
    }

    public boolean hasLocation() {
        return hasCity() || hasState() || hasCountry() || hasZipCode();
    }

    public boolean hasPhoneNumber() {
        return StringUtils.isNotEmpty(phoneNumber);
    }

    public boolean hasDuns() {
        return StringUtils.isNotEmpty(duns);
    }

    public boolean hasEmail() {
        return StringUtils.isNotEmpty(email);
    }

    @Override
    public String toString() {
        if (serializedFormat == null) {
            log.error("$JAW$ serializedFormat was null!  Refreshing cached strings");
            refreshCachedStrings();
        }
        return serializedFormat;
    }

    /**
     * Return a unique string to represent the combination of {@link MatchKey}s
     * of this tuple
     *
     * Note that {@link MatchKeyTuple#getCountryCode()} is not evaluated and
     * should be in sync with country
     *
     * @return generated unique string
     */
    public String buildIdForKey() {
        return uniqueIdForKey;
    }

    /**
     * Return a unique string to represent the {@link MatchKey} and value pairs
     * of this tuple Note that {@link MatchKeyTuple#getCountryCode()} is not
     * evaluated and should be in sync with country
     *
     * @return generated unique string
     */
    public String buildIdForValue() {
        return uniqueIdForValue;
    }

    private void refreshCachedStrings() {
        constructSerializedFormat();
        constructIdForKey();
        constructIdForValue();
    }

    // TODO(ZDD) Change MatchConstants to MatchKey?
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
        if (CollectionUtils.isNotEmpty(systemIds)) {
            sb.append(String.format("%s=%s, ", MatchKey.SystemId.name(), systemIds.toString()));
        }
        sb.append(")");
        serializedFormat = sb.toString();
    }

    /*
     * refresh the cached unique ID that represents non-empty match key
     * combination in this tuple
     */
    private void constructIdForKey() {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(duns)) {
            appendKey(sb, MatchKey.DUNS.name());
        }
        if (StringUtils.isNotEmpty(domain)) {
            appendKey(sb, MatchKey.Domain.name());
        }
        if (StringUtils.isNotEmpty(name)) {
            appendKey(sb, MatchKey.Name.name());
        }
        if (StringUtils.isNotEmpty(city)) {
            appendKey(sb, MatchKey.City.name());
        }
        if (StringUtils.isNotEmpty(state)) {
            appendKey(sb, MatchKey.State.name());
        }
        if (StringUtils.isNotEmpty(zipcode)) {
            appendKey(sb, MatchKey.Zipcode.name());
        }
        if (StringUtils.isNotEmpty(country)) {
            appendKey(sb, MatchKey.Country.name());
        }
        // NOTE no country code
        if (StringUtils.isNotEmpty(phoneNumber)) {
            appendKey(sb, MatchKey.PhoneNumber.name());
        }
        if (StringUtils.isNotEmpty(email)) {
            appendKey(sb, MatchKey.Email.name());
        }
        uniqueIdForKey = sb.toString();
    }

    /*
     * refresh the cached unique ID that represents non-empty match key/value
     * pairs in this tuple
     */
    private void constructIdForValue() {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(duns)) {
            appendKeyValue(sb, MatchKey.DUNS.name(), duns);
        }
        if (StringUtils.isNotEmpty(domain)) {
            appendKeyValue(sb, MatchKey.Domain.name(), domain);
        }
        if (StringUtils.isNotEmpty(name)) {
            appendKeyValue(sb, MatchKey.Name.name(), name);
        }
        if (StringUtils.isNotEmpty(city)) {
            appendKeyValue(sb, MatchKey.City.name(), city);
        }
        if (StringUtils.isNotEmpty(state)) {
            appendKeyValue(sb, MatchKey.State.name(), state);
        }
        if (StringUtils.isNotEmpty(zipcode)) {
            appendKeyValue(sb, MatchKey.Zipcode.name(), zipcode);
        }
        if (StringUtils.isNotEmpty(country)) {
            appendKeyValue(sb, MatchKey.Country.name(), country);
        }
        // NOTE no country code
        if (StringUtils.isNotEmpty(phoneNumber)) {
            appendKeyValue(sb, MatchKey.PhoneNumber.name(), phoneNumber);
        }
        if (StringUtils.isNotEmpty(email)) {
            appendKeyValue(sb, MatchKey.Email.name(), email);
        }
        uniqueIdForValue = sb.toString();
    }

    private void appendKey(StringBuilder sb, String key) {
        if (sb.length() > 0) {
            sb.append(",");
        }
        sb.append(key);
    }

    private void appendKeyValue(StringBuilder sb, String key, String value) {
        if (sb.length() > 0) {
            sb.append(",");
        }
        sb.append(String.format("%s=%s", key, value));
    }

    /*
     * generated builder
     */
    public static final class Builder {
        private MatchKeyTuple matchKeyTuple;

        public Builder() {
            matchKeyTuple = new MatchKeyTuple();
        }

        public Builder withDomain(String domain) {
            matchKeyTuple.setDomain(domain);
            return this;
        }

        public Builder withName(String name) {
            matchKeyTuple.setName(name);
            return this;
        }

        public Builder withCity(String city) {
            matchKeyTuple.setCity(city);
            return this;
        }

        public Builder withState(String state) {
            matchKeyTuple.setState(state);
            return this;
        }

        public Builder withCountry(String country) {
            matchKeyTuple.setCountry(country);
            return this;
        }

        public Builder withCountryCode(String countryCode) {
            matchKeyTuple.setCountryCode(countryCode);
            return this;
        }

        public Builder withZipcode(String zipcode) {
            matchKeyTuple.setZipcode(zipcode);
            return this;
        }

        public Builder withPhoneNumber(String phoneNumber) {
            matchKeyTuple.setPhoneNumber(phoneNumber);
            return this;
        }

        public Builder withDuns(String duns) {
            matchKeyTuple.setDuns(duns);
            return this;
        }

        public Builder withEmail(String email) {
            matchKeyTuple.setEmail(email);
            return this;
        }

        public Builder withSystemIds(List<Pair<String, String>> systemIds) {
            matchKeyTuple.setSystemIds(systemIds);
            return this;
        }

        public MatchKeyTuple build() {
            return matchKeyTuple;
        }
    }
}
