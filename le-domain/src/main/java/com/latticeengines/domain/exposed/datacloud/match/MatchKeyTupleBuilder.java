package com.latticeengines.domain.exposed.datacloud.match;

/**
 * Builder for {@link MatchKeyTuple}
 */
public final class MatchKeyTupleBuilder {
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

    private MatchKeyTupleBuilder() {
    }

    public static MatchKeyTupleBuilder newBuilder() {
        return new MatchKeyTupleBuilder();
    }

    public MatchKeyTupleBuilder withDomain(String domain) {
        this.domain = domain;
        return this;
    }

    public MatchKeyTupleBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public MatchKeyTupleBuilder withCity(String city) {
        this.city = city;
        return this;
    }

    public MatchKeyTupleBuilder withState(String state) {
        this.state = state;
        return this;
    }

    public MatchKeyTupleBuilder withCountry(String country) {
        this.country = country;
        return this;
    }

    public MatchKeyTupleBuilder withCountryCode(String countryCode) {
        this.countryCode = countryCode;
        return this;
    }

    public MatchKeyTupleBuilder withZipcode(String zipcode) {
        this.zipcode = zipcode;
        return this;
    }

    public MatchKeyTupleBuilder withPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        return this;
    }

    public MatchKeyTupleBuilder withDuns(String duns) {
        this.duns = duns;
        return this;
    }

    public MatchKeyTupleBuilder withEmail(String email) {
        this.email = email;
        return this;
    }

    public MatchKeyTuple build() {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        matchKeyTuple.setDomain(domain);
        matchKeyTuple.setName(name);
        matchKeyTuple.setCity(city);
        matchKeyTuple.setState(state);
        matchKeyTuple.setCountry(country);
        matchKeyTuple.setCountryCode(countryCode);
        matchKeyTuple.setZipcode(zipcode);
        matchKeyTuple.setPhoneNumber(phoneNumber);
        matchKeyTuple.setDuns(duns);
        matchKeyTuple.setEmail(email);
        return matchKeyTuple;
    }
}
