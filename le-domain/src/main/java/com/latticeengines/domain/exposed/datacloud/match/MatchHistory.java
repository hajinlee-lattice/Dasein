package com.latticeengines.domain.exposed.datacloud.match;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.Union;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public class MatchHistory implements HasId<String> {

    @Nullable
    @AvroName("Id")
    private String id;

    @Nullable
    @AvroName("Domain")
    private String domain;
    @Nullable
    @AvroName("DUNS")
    private String duns;
    @Nullable
    @AvroName("Email")
    private String email;

    @Nullable
    @AvroName("IsPublicDomain")
    private Boolean isPublicDomain;

    @Nullable
    @AvroName("Name")
    private String name;
    @Nullable
    @AvroName("City")
    private String city;
    @Nullable
    @AvroName("Country")
    private String country;
    @Nullable
    @AvroName("CountryCode")
    private String countryCode;
    @Nullable
    @AvroName("PhoneNumber")
    private String phoneNumber;
    @Nullable
    @AvroName("State")
    private String state;
    @Nullable
    @AvroName("Street")
    private String street;
    @Nullable
    @AvroName("Zipcode")
    private String zipcode;

    @Nullable
    @AvroName("Matched")
    private Boolean matched;

    @Nullable
    @AvroName("LatticeAccountId")
    private String latticeAccountId;

    @Nullable
    @AvroName("Timestamp")
    private Long timestampSeconds;

    @Override
    @Union({})
    public String getId() {
        return id;
    }

    @Override
    @Union({})
    public void setId(String id) {
        this.id = id;
    }

    @Union({})
    public String getName() {
        return name;
    }

    @Union({})
    public MatchHistory setName(String name) {
        this.name = name;
        return this;
    }

    @Union({})
    public String getCity() {
        return city;
    }

    @Union({})
    public MatchHistory setCity(String city) {
        this.city = city;
        return this;
    }

    @Union({})
    public String getCountry() {
        return country;
    }

    @Union({})
    public MatchHistory setCountry(String country) {
        this.country = country;
        return this;
    }

    @Union({})
    public String getCountryCode() {
        return countryCode;
    }

    @Union({})
    public MatchHistory setCountryCode(String countryCode) {
        this.countryCode = countryCode;
        return this;
    }

    @Union({})
    public String getPhoneNumber() {
        return phoneNumber;
    }

    @Union({})
    public MatchHistory setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        return this;
    }

    @Union({})
    public String getState() {
        return state;
    }

    @Union({})
    public MatchHistory setState(String state) {
        this.state = state;
        return this;
    }

    @Union({})
    public String getStreet() {
        return street;
    }

    @Union({})
    public MatchHistory setStreet(String street) {
        this.street = street;
        return this;
    }

    @Union({})
    public String getZipcode() {
        return zipcode;
    }

    @Union({})
    public MatchHistory setZipcode(String zipcode) {
        this.zipcode = zipcode;
        return this;
    }

    @Union({})
    public Boolean getMatched() {
        return matched;
    }

    @Union({})
    public MatchHistory setMatched(Boolean matched) {
        this.matched = matched;
        return this;
    }

    @Union({})
    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    @Union({})
    public MatchHistory setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
        return this;
    }

    @Union({})
    public String getDomain() {
        return domain;
    }

    @Union({})
    public String getDuns() {
        return duns;
    }

    @Union({})
    public String getEmail() {
        return email;
    }

    @Union({})
    public MatchHistory setDomain(String domain) {
        this.domain = domain;
        return this;
    }

    @Union({})
    public MatchHistory setDuns(String duns) {
        this.duns = duns;
        return this;
    }

    @Union({})
    public MatchHistory setEmail(String email) {
        this.email = email;
        return this;
    }

    @Union({})
    public Boolean getIsPublicDomain() {
        return isPublicDomain;
    }

    @Union({})
    public MatchHistory setIsPublicDomain(Boolean isPublicDomain) {
        this.isPublicDomain = isPublicDomain;
        return this;
    }

    @Union({})
    public Long getTimestamp() {
        return timestampSeconds;
    }

    @Union({})
    public void setTimestampSeconds(Long timestampSeconds) {
        this.timestampSeconds = timestampSeconds;
    }

    public MatchHistory withNameLocation(NameLocation nameLocation) {
        if (nameLocation == null) {
            return this;
        }
        this.name = nameLocation.getName();
        this.city = nameLocation.getCity();
        this.country = nameLocation.getCountry();
        this.countryCode = nameLocation.getCountryCode();
        this.phoneNumber = nameLocation.getPhoneNumber();
        this.state = nameLocation.getState();
        this.street = nameLocation.getStreet();
        this.zipcode = nameLocation.getZipcode();
        return this;
    }

}
