package com.latticeengines.domain.exposed.datacloud.match;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.Union;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class MatchHistory implements HasId<String> {

    @Nullable
    @AvroName("ID")
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
    @AvroName("TimestampSeconds")
    private Long timestampSeconds;
    @Nullable
    @AvroName("HitWhiteCache")
    private Boolean hitWhiteCache;
    @Nullable
    @AvroName("MatchMode")
    private String matchMode;

    @Nullable
    @AvroName("MatchedDuns")
    private String matchedDuns;
    @Nullable
    @AvroName("MatchedConfidenceCode")
    private Integer matchedConfidenceCode;
    @Nullable
    @AvroName("MatchedMatchGrade")
    private String matchedMatchGrade;
    @Nullable
    @AvroName("MatchedReturnCode")
    private String matchedReturnCode;

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
    public String getCity() {
        return city;
    }

    @Union({})
    public String getCountry() {
        return country;
    }

    @Union({})
    public String getCountryCode() {
        return countryCode;
    }

    @Union({})
    public String getPhoneNumber() {
        return phoneNumber;
    }

    @Union({})
    public String getState() {
        return state;
    }

    @Union({})
    public String getStreet() {
        return street;
    }

    @Union({})
    public String getZipcode() {
        return zipcode;
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
    public MatchHistory setDomain(String domain) {
        this.domain = domain;
        return this;
    }

    @Union({})
    public String getDuns() {
        return duns;
    }

    @Union({})
    public MatchHistory setDuns(String duns) {
        this.duns = duns;
        return this;
    }

    @Union({})
    public String getEmail() {
        return email;
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
    public Long getTimestampSeconds() {
        return this.timestampSeconds;
    }

    @Union({})
    public MatchHistory setTimestampSeconds(Long timestampSeconds) {
        this.timestampSeconds = timestampSeconds;
        return this;
    }

    @Union({})
    public Boolean getHitWhiteCache() {
        return hitWhiteCache;
    }

    @Union({})
    public String getMatchedDuns() {
        return matchedDuns;
    }

    @Union({})
    public Integer getMatchedConfidenceCode() {
        return matchedConfidenceCode;
    }

    @Union({})
    public Object getMatchedMatchGrade() {
        return matchedMatchGrade;
    }

    @Union({})
    public String getMatchedReturnCode() {
        return matchedReturnCode;
    }

    @Union({})
    public String getMatchMode() {
        return matchMode;
    }

    @Union({})
    public MatchHistory setMatchMode(String mode) {
        this.matchMode = mode;
        return this;
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

    public MatchHistory withDnBMatchResult(DnBMatchContext matchContext) {
        this.matchedDuns = matchContext.getDuns();
        this.matchedConfidenceCode = matchContext.getConfidenceCode();
        this.matchedMatchGrade = matchContext.getMatchGrade() != null
                && matchContext.getMatchGrade().getRawCode() != null ? matchContext.getMatchGrade().getRawCode() : null;
        this.hitWhiteCache = matchContext.getHitWhiteCache();
        if (matchContext.getDnbCode() != null)
            this.matchedReturnCode = matchContext.getDnbCode().toString();
        return this;
    }
}
