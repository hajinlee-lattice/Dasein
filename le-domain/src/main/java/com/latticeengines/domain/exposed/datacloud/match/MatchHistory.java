package com.latticeengines.domain.exposed.datacloud.match;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.Union;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class MatchHistory implements HasId<String> {

    @Nullable
    @AvroName("TypeOfApplication")
    String typeOfApplication; // Ex: Marketo, LPI, SDFC
    @Nullable
    @AvroName("TypeOfJob")
    String requestSource; // Ex: Scoring, Modeling, Enrichment
    @Nullable
    @AvroName("TenantId")
    String tenantId;
    @Nullable
    @AvroName("RootOperationUid")
    String rootOperationUid;
    @Nullable
    @AvroName("RequestTimestamp")
    String requestTimestamp;
    @Nullable
    @AvroName("LatticeAccountId")
    String latticeAccountId;
    @Nullable
    @AvroName("RawDomain")
    String rawDomain;
    @Nullable
    @AvroName("RawEmail")
    String rawEmail;
    @Nullable
    @AvroName("RawCompanyName")
    String rawCompanyName;
    @Nullable
    @AvroName("RawCity")
    String rawCity;
    @Nullable
    @AvroName("RawState")
    String rawState;
    @Nullable
    @AvroName("RawStreet")
    String rawStreet;
    @Nullable
    @AvroName("RawPostalCode")
    String rawPostalCode;
    @Nullable
    @AvroName("RawCountry")
    String rawCountry;
    @Nullable
    @AvroName("RawCountryCode")
    String rawCountryCode;
    @Nullable
    @AvroName("RawPhone")
    String rawPhone;
    @Nullable
    @AvroName("RawDUNS")
    String rawDUNS;
    @Nullable
    @AvroName("StandardisedDomain")
    String standardisedDomain;
    @Nullable
    @AvroName("StandardisedDUNS")
    String standardisedDUNS;
    @Nullable
    @AvroName("StandardisedEmail")
    String standardisedEmail;
    @Nullable
    @AvroName("StandardisedCompanyName")
    String standardisedCompanyName;
    @Nullable
    @AvroName("StandardisedCity")
    String standardisedCity;
    @Nullable
    @AvroName("StandardisedState")
    String standardisedState;
    @Nullable
    @AvroName("StandardisedStreet")
    String standardisedStreet;
    @Nullable
    @AvroName("StandardisedPostalCode")
    String standardisedPostalCode;
    @Nullable
    @AvroName("StandardisedCountry")
    String standardisedCountry;
    @Nullable
    @AvroName("StandardisedCountryCode")
    String standardisedCountryCode;
    @Nullable
    @AvroName("StandardisedPhone")
    String standardisedPhone;
    @Nullable
    @AvroName("DnbMatchedDUNS")
    String dnbMatchedDUNS;
    @Nullable
    @AvroName("DnbMatchedDomain")
    String dnbMatchedDomain;
    @Nullable
    @AvroName("DnbMatchedEmail")
    String dnbMatchedEmail;
    @Nullable
    @AvroName("DnbMatchedCompanyName")
    String dnbMatchedCompanyName;
    @Nullable
    @AvroName("DnbMatchedCity")
    String dnbMatchedCity;
    @Nullable
    @AvroName("DnbMatchedState")
    String dnbMatchedState;
    @Nullable
    @AvroName("DnbMatchedStreet")
    String dnbMatchedStreet;
    @Nullable
    @AvroName("DnbMatchedPostalCode")
    String dnbMatchedPostalCode;
    @Nullable
    @AvroName("DnbMatchedCountry")
    String dnbMatchedCountry;
    @Nullable
    @AvroName("DnbMatchedCountryCode")
    String dnbMatchedCountryCode;
    @Nullable
    @AvroName("DnbMatchedPhone")
    String dnbMatchedPhone;
    @Nullable
    @AvroName("MatchedDUNS")
    String matchedDUNS;
    @Nullable
    @AvroName("MatchedDomain")
    String matchedDomain;
    @Nullable
    @AvroName("MatchedEmail")
    String matchedEmail;
    @Nullable
    @AvroName("MatchedCompanyName")
    String matchedCompanyName;
    @Nullable
    @AvroName("MatchedCity")
    String matchedCity;
    @Nullable
    @AvroName("MatchedState")
    String matchedState;
    @Nullable
    @AvroName("MatchedStreet")
    String matchedStreet;
    @Nullable
    @AvroName("MatchedPostalCode")
    String matchedPostalCode;
    @Nullable
    @AvroName("MatchedCountry")
    String matchedCountry;
    @Nullable
    @AvroName("MatchedCountryCode")
    String matchedCountryCode;
    @Nullable
    @AvroName("MatchedPhone")
    String matchedPhone;
    @Nullable
    @AvroName("MatchedPrimaryIndustry")
    String matchedPrimaryIndustry;
    @Nullable
    @AvroName("MatchedSecondaryIndustry")
    String matchedSecondaryIndustry;
    @Nullable
    @AvroName("MatchedEmployeeRange")
    String matchedEmployeeRange;
    @Nullable
    @AvroName("MatchedRevenueRange")
    String matchedRevenueRange;
    @Nullable
    @AvroName("ConfidenceCode")
    Integer confidenceCode;
    @Nullable
    @AvroName("MatchGrade")
    String matchGrade;
    @Nullable
    @AvroName("NameMatchGradeText")
    String nameMatchGradeText;
    @Nullable
    @AvroName("CityMatchGradeText")
    String cityMatchGradeText;
    @Nullable
    @AvroName("StateMatchGradeText")
    String stateMatchGradeText;
    @Nullable
    @AvroName("PostalCodeMatchGradeText")
    String postalCodeMatchGradeText;
    @Nullable
    @AvroName("PhoneMatchGradeText")
    String phoneMatchGradeText;
    @Nullable
    @AvroName("IsDnBMatch")
    Boolean isDnBMatch;
    @Nullable
    @AvroName("DomainSource")
    String domainSource; // Ex: LE, DnB
    @Nullable
    @AvroName("IsPublicDoimain")
    Boolean isPublicDomain;
    @Nullable
    @AvroName("MatchRetrievalTime")
    String matchRetrievalTime;
    @Nullable
    @AvroName("RemoteDnbAPICall")
    Boolean remoteDnbAPICall;
    @Nullable
    @AvroName("RemoteDnbAPIRequestTime")
    String remoteDnbAPIRequestTime;
    @Nullable
    @AvroName("RemoteDnbAPIResponseTime")
    String remoteDnbAPIResponseTime;
    @Nullable
    @AvroName("PassedAcceptanceCriteria")
    Boolean passedAcceptanceCriteria;
    @Nullable
    @AvroName("DnbReturnCode")
    String dnbReturnCode;
    @Nullable
    @AvroName("ID")
    private String id;
    @Nullable
    @AvroName("Matched")
    private Boolean matched;
    @Nullable
    @AvroName("HitWhiteCache")
    private Boolean hitWhiteCache;
    @Nullable
    @AvroName("HitBlackCache")
    private Boolean hitBlackCache;
    @Nullable
    @AvroName("MatchMode")
    private String matchMode; // Batch or Realtime

    // Temporary: Entity Match Result

    @JsonProperty("EntityMatchHistory")
    @Nullable
    @AvroName("EntityMatchHistory")
    private EntityMatchHistory entityMatchHistory;

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
    public Boolean getMatched() {
        return matched;
    }

    @Union({})
    public MatchHistory setMatched(Boolean matched) {
        this.matched = matched;
        return this;
    }

    @Union({})
    public Boolean getHitWhiteCache() {
        return hitWhiteCache;
    }

    @Union({})
    public MatchHistory setHitWhiteCache(Boolean hitWhiteCache) {
        this.hitWhiteCache = hitWhiteCache;
        return this;
    }

    @Union({})
    public Boolean getHitBlackCache() {
        return hitBlackCache;
    }

    @Union({})
    public MatchHistory setHitBlackCache(Boolean hitBlackCache) {
        this.hitBlackCache = hitBlackCache;
        return this;
    }

    @Union({})
    public String getMatchMode() {
        return matchMode;
    }

    @Union({})
    public MatchHistory setMatchMode(String matchMode) {
        this.matchMode = matchMode;
        return this;
    }

    @Union({})
    public String getTypeOfApplication() {
        return typeOfApplication;
    }

    @Union({})
    public MatchHistory setTypeOfApplication(String typeOfApplication) {
        this.typeOfApplication = typeOfApplication;
        return this;
    }

    @Union({})
    public String getRequestSource() {
        return requestSource;
    }

    @Union({})
    public MatchHistory setRequestSource(String requestSource) {
        this.requestSource = requestSource;
        return this;
    }

    @Union({})
    public String getTenantId() {
        return tenantId;
    }

    @Union({})
    public MatchHistory setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    @Union({})
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @Union({})
    public MatchHistory setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
        return this;
    }

    @Union({})
    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    @Union({})
    public MatchHistory setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
        return this;
    }

    @Union({})
    public String getRawDomain() {
        return rawDomain;
    }

    @Union({})
    public MatchHistory setRawDomain(String rawDomain) {
        this.rawDomain = rawDomain;
        return this;
    }

    @Union({})
    public String getRawEmail() {
        return rawEmail;
    }

    @Union({})
    public MatchHistory setRawEmail(String rawEmail) {
        this.rawEmail = rawEmail;
        return this;
    }

    @Union({})
    public String getRawCompanyName() {
        return rawCompanyName;
    }

    @Union({})
    public MatchHistory setRawCompanyName(String rawCompanyName) {
        this.rawCompanyName = rawCompanyName;
        return this;
    }

    @Union({})
    public String getRawCity() {
        return rawCity;
    }

    @Union({})
    public MatchHistory setRawCity(String rawCity) {
        this.rawCity = rawCity;
        return this;
    }

    @Union({})
    public String getRawState() {
        return rawState;
    }

    @Union({})
    public MatchHistory setRawState(String rawState) {
        this.rawState = rawState;
        return this;
    }

    @Union({})
    public String getRawPostalCode() {
        return rawPostalCode;
    }

    @Union({})
    public MatchHistory setRawPostalCode(String rawPostalCode) {
        this.rawPostalCode = rawPostalCode;
        return this;
    }

    @Union({})
    public String getRawCountry() {
        return rawCountry;
    }

    @Union({})
    public MatchHistory setRawCountry(String rawCountry) {
        this.rawCountry = rawCountry;
        return this;
    }

    @Union({})
    public String getRawPhone() {
        return rawPhone;
    }

    @Union({})
    public MatchHistory setRawPhone(String rawPhone) {
        this.rawPhone = rawPhone;
        return this;
    }

    @Union({})
    public String getRawDUNS() {
        return rawDUNS;
    }

    @Union({})
    public MatchHistory setRawDUNS(String rawDUNS) {
        this.rawDUNS = rawDUNS;
        return this;
    }

    @Union({})
    public String getStandardisedDomain() {
        return standardisedDomain;
    }

    @Union({})
    public MatchHistory setStandardisedDomain(String standardisedDomain) {
        this.standardisedDomain = standardisedDomain;
        return this;
    }

    @Union({})
    public String getStandardisedEmail() {
        return standardisedEmail;
    }

    @Union({})
    public MatchHistory setStandardisedEmail(String standardisedEmail) {
        this.standardisedEmail = standardisedEmail;
        return this;
    }

    @Union({})
    public String getStandardisedCompanyName() {
        return standardisedCompanyName;
    }

    @Union({})
    public MatchHistory setStandardisedCompanyName(String standardisedCompanyName) {
        this.standardisedCompanyName = standardisedCompanyName;
        return this;
    }

    @Union({})
    public String getStandardisedCity() {
        return standardisedCity;
    }

    @Union({})
    public MatchHistory setStandardisedCity(String standardisedCity) {
        this.standardisedCity = standardisedCity;
        return this;
    }

    @Union({})
    public String getStandardisedState() {
        return standardisedState;
    }

    @Union({})
    public MatchHistory setStandardisedState(String standardisedState) {
        this.standardisedState = standardisedState;
        return this;
    }

    @Union({})
    public String getStandardisedPostalCode() {
        return standardisedPostalCode;
    }

    @Union({})
    public MatchHistory setStandardisedPostalCode(String standardisedPostalCode) {
        this.standardisedPostalCode = standardisedPostalCode;
        return this;
    }

    @Union({})
    public String getStandardisedCountry() {
        return standardisedCountry;
    }

    @Union({})
    public MatchHistory setStandardisedCountry(String standardisedCountry) {
        this.standardisedCountry = standardisedCountry;
        return this;
    }

    @Union({})
    public String getStandardisedPhone() {
        return standardisedPhone;
    }

    @Union({})
    public MatchHistory setStandardisedPhone(String standardisedPhone) {
        this.standardisedPhone = standardisedPhone;
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
    public String getDnbMatchedDUNS() {
        return dnbMatchedDUNS;
    }

    @Union({})
    public MatchHistory setDnbMatchedDUNS(String dnbMatchedDUNS) {
        this.dnbMatchedDUNS = dnbMatchedDUNS;
        return this;
    }

    @Union({})
    public String getDnbMatchedDomain() {
        return dnbMatchedDomain;
    }

    @Union({})
    public MatchHistory setDnbMatchedDomain(String dnbMatchedDomain) {
        this.dnbMatchedDomain = dnbMatchedDomain;
        return this;
    }

    @Union({})
    public String getDnbMatchedEmail() {
        return dnbMatchedEmail;
    }

    @Union({})
    public MatchHistory setDnbMatchedEmail(String dnbMatchedEmail) {
        this.dnbMatchedEmail = dnbMatchedEmail;
        return this;
    }

    @Union({})
    public String getDnbMatchedCompanyName() {
        return dnbMatchedCompanyName;
    }

    @Union({})
    public MatchHistory setDnbMatchedCompanyName(String dnbMatchedCompanyName) {
        this.dnbMatchedCompanyName = dnbMatchedCompanyName;
        return this;
    }

    @Union({})
    public String getDnbMatchedCity() {
        return dnbMatchedCity;
    }

    @Union({})
    public MatchHistory setDnbMatchedCity(String dnbMatchedCity) {
        this.dnbMatchedCity = dnbMatchedCity;
        return this;
    }

    @Union({})
    public String getDnbMatchedState() {
        return dnbMatchedState;
    }

    @Union({})
    public MatchHistory setDnbMatchedState(String dnbMatchedState) {
        this.dnbMatchedState = dnbMatchedState;
        return this;
    }

    @Union({})
    public String getDnbMatchedPostalCode() {
        return dnbMatchedPostalCode;
    }

    @Union({})
    public MatchHistory setDnbMatchedPostalCode(String dnbMatchedPostalCode) {
        this.dnbMatchedPostalCode = dnbMatchedPostalCode;
        return this;
    }

    @Union({})
    public String getDnbMatchedCountry() {
        return dnbMatchedCountry;
    }

    @Union({})
    public MatchHistory setDnbMatchedCountry(String dnbMatchedCountry) {
        this.dnbMatchedCountry = dnbMatchedCountry;
        return this;
    }

    @Union({})
    public String getDnbMatchedCountryCode() {
        return dnbMatchedCountryCode;
    }

    @Union({})
    public MatchHistory setDnbMatchedCountryCode(String dnbMatchedCountryCode) {
        this.dnbMatchedCountryCode = dnbMatchedCountryCode;
        return this;
    }

    @Union({})
    public String getDnbMatchedPhone() {
        return dnbMatchedPhone;
    }

    @Union({})
    public MatchHistory setDnbMatchedPhone(String dnbMatchedPhone) {
        this.dnbMatchedPhone = dnbMatchedPhone;
        return this;
    }

    @Union({})
    public String getDnBMatchedStreet() {
        return dnbMatchedStreet;
    }

    @Union({})
    public MatchHistory setDnbMatchedStreet(String dnbMatchedStreet) {
        this.dnbMatchedStreet = dnbMatchedStreet;
        return this;
    }

    @Union({})
    public String getMatchedDUNS() {
        return matchedDUNS;
    }

    @Union({})
    public MatchHistory setMatchedDUNS(String matchedDUNS) {
        this.matchedDUNS = matchedDUNS;
        return this;
    }

    @Union({})
    public String getMatchedDomain() {
        return matchedDomain;
    }

    @Union({})
    public MatchHistory setMatchedDomain(String matchedDomain) {
        this.matchedDomain = matchedDomain;
        return this;
    }

    @Union({})
    public String getMatchedEmail() {
        return matchedEmail;
    }

    @Union({})
    public MatchHistory setMatchedEmail(String matchedEmail) {
        this.matchedEmail = matchedEmail;
        return this;
    }

    @Union({})
    public String getMatchedCompanyName() {
        return matchedCompanyName;
    }

    @Union({})
    public MatchHistory setMatchedCompanyName(String matchedCompanyName) {
        this.matchedCompanyName = matchedCompanyName;
        return this;
    }

    @Union({})
    public String getMatchedCity() {
        return matchedCity;
    }

    @Union({})
    public MatchHistory setMatchedCity(String matchedCity) {
        this.matchedCity = matchedCity;
        return this;
    }

    @Union({})
    public String getMatchedState() {
        return matchedState;
    }

    @Union({})
    public MatchHistory setMatchedState(String matchedState) {
        this.matchedState = matchedState;
        return this;
    }

    @Union({})
    public String getMatchedPostalCode() {
        return matchedPostalCode;
    }

    @Union({})
    public MatchHistory setMatchedPostalCode(String matchedPostalCode) {
        this.matchedPostalCode = matchedPostalCode;
        return this;
    }

    @Union({})
    public String getMatchedCountry() {
        return matchedCountry;
    }

    @Union({})
    public MatchHistory setMatchedCountry(String matchedCountry) {
        this.matchedCountry = matchedCountry;
        return this;
    }

    @Union({})
    public String getMatchedPhone() {
        return matchedPhone;
    }

    @Union({})
    public MatchHistory setMatchedPhone(String matchedPhone) {
        this.matchedPhone = matchedPhone;
        return this;
    }

    @Union({})
    public String getMatchedStreet() {
        return matchedStreet;
    }

    @Union({})
    public MatchHistory setMatchedStreet(String matchedStreet) {
        this.matchedStreet = matchedStreet;
        return this;
    }

    @Union({})
    public String getMatchedCountryCode() {
        return matchedCountryCode;
    }

    @Union({})
    public MatchHistory setMatchedCountryCode(String matchedCountryCode) {
        this.matchedCountryCode = matchedCountryCode;
        return this;
    }

    @Union({})
    public String getMatchedPrimaryIndustry() {
        return matchedPrimaryIndustry;
    }

    @Union({})
    public MatchHistory setMatchedPrimaryIndustry(String matchedPrimaryIndustry) {
        this.matchedPrimaryIndustry = matchedPrimaryIndustry;
        return this;
    }

    @Union({})
    public String getMatchedSecondaryIndustry() {
        return matchedSecondaryIndustry;
    }

    @Union({})
    public MatchHistory setMatchedSecondaryIndustry(String matchedSecondaryIndustry) {
        this.matchedSecondaryIndustry = matchedSecondaryIndustry;
        return this;
    }

    @Union({})
    public String getMatchedEmployeeRange() {
        return matchedEmployeeRange;
    }

    @Union({})
    public MatchHistory setMatchedEmployeeRange(String matchedEmployeeRange) {
        this.matchedEmployeeRange = matchedEmployeeRange;
        return this;
    }

    @Union({})
    public String getMatchedRevenueRange() {
        return matchedRevenueRange;
    }

    @Union({})
    public MatchHistory setMatchedRevenueRange(String matchedRevenueRange) {
        this.matchedRevenueRange = matchedRevenueRange;
        return this;
    }

    @Union({})
    public Integer getConfidenceCode() {
        return confidenceCode;
    }

    @Union({})
    public MatchHistory setConfidenceCode(Integer confidenceCode) {
        this.confidenceCode = confidenceCode;
        return this;
    }

    @Union({})
    public String getMatchGrade() {
        return matchGrade;
    }

    @Union({})
    public MatchHistory setMatchGrade(String matchGrade) {
        this.matchGrade = matchGrade;
        return this;
    }

    @Union({})
    public String getNameMatchGradeText() {
        return nameMatchGradeText;
    }

    @Union({})
    public MatchHistory setNameMatchGradeText(String nameMatchGradeText) {
        this.nameMatchGradeText = nameMatchGradeText;
        return this;
    }

    @Union({})
    public String getCityMatchGradeText() {
        return cityMatchGradeText;
    }

    @Union({})
    public MatchHistory setCityMatchGradeText(String cityMatchGradeText) {
        this.cityMatchGradeText = cityMatchGradeText;
        return this;
    }

    @Union({})
    public String getStateMatchGradeText() {
        return stateMatchGradeText;
    }

    @Union({})
    public MatchHistory setStateMatchGradeText(String stateMatchGradeText) {
        this.stateMatchGradeText = stateMatchGradeText;
        return this;
    }

    @Union({})
    public String getPostalCodeMatchGradeText() {
        return postalCodeMatchGradeText;
    }

    @Union({})
    public MatchHistory setPostalCodeMatchGradeText(String postalCodeMatchGradeText) {
        this.postalCodeMatchGradeText = postalCodeMatchGradeText;
        return this;
    }

    @Union({})
    public String getPhoneMatchGradeText() {
        return phoneMatchGradeText;
    }

    @Union({})
    public MatchHistory setPhoneMatchGradeText(String phoneMatchGradeText) {
        this.phoneMatchGradeText = phoneMatchGradeText;
        return this;
    }

    @Union({})
    public Boolean getIsDnBMatch() {
        return isDnBMatch;
    }

    @Union({})
    public MatchHistory setIsDnBMatch(Boolean isDnbMatch) {
        this.isDnBMatch = isDnbMatch;
        return this;
    }

    @Union({})
    public String getDomainSource() {
        return domainSource;
    }

    @Union({})
    public MatchHistory setDomainSource(String domainSource) {
        this.domainSource = domainSource;
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
    public String getMatchRetrievalTime() {
        return matchRetrievalTime;
    }

    @Union({})
    public MatchHistory setMatchRetrievalTime(String matchRetrievalTime) {
        this.matchRetrievalTime = matchRetrievalTime;
        return this;
    }

    @Union({})
    public Boolean getRemoteDnbAPICall() {
        return remoteDnbAPICall;
    }

    @Union({})
    public MatchHistory setRemoteDnbAPICall(Boolean remoteDnbAPICall) {
        this.remoteDnbAPICall = remoteDnbAPICall;
        return this;
    }

    @Union({})
    public String getRemoteDnbAPIRequestTime() {
        return remoteDnbAPIRequestTime;
    }

    @Union({})
    public MatchHistory setRemoteDnbAPIRequestTime(String remoteDnbAPIRequestTime) {
        this.remoteDnbAPIRequestTime = remoteDnbAPIRequestTime;
        return this;
    }

    @Union({})
    public String getRemoteDnbAPIResponseTime() {
        return remoteDnbAPIResponseTime;
    }

    @Union({})
    public MatchHistory setRemoteDnbAPIResponseTime(String remoteDnbAPIResponseTime) {
        this.remoteDnbAPIResponseTime = remoteDnbAPIResponseTime;
        return this;
    }

    @Union({})
    public Boolean getPassedAcceptanceCriteria() {
        return passedAcceptanceCriteria;
    }

    @Union({})
    public MatchHistory setPassedAcceptanceCriteria(Boolean passedAcceptanceCriteria) {
        this.passedAcceptanceCriteria = passedAcceptanceCriteria;
        return this;
    }

    @Union({})
    public String getDnbReturnCode() {
        return dnbReturnCode;
    }

    @Union({})
    public MatchHistory setDnbReturnCode(String dnbReturnCode) {
        this.dnbReturnCode = dnbReturnCode;
        return this;
    }

    @Union({})
    public String getRawStreet() {
        return rawStreet;
    }

    @Union({})
    public MatchHistory setRawStreet(String rawStreet) {
        this.rawStreet = rawStreet;
        return this;
    }

    @Union({})
    public String getStandardisedStreet() {
        return standardisedStreet;
    }

    @Union({})
    public MatchHistory setStandardisedStreet(String standardisedStreet) {
        this.standardisedStreet = standardisedStreet;
        return this;
    }

    @Union({})
    public String getStandardisedDUNS() {
        return standardisedDUNS;
    }

    @Union({})
    public MatchHistory setStandardisedDUNS(String standardisedDUNS) {
        this.standardisedDUNS = standardisedDUNS;
        return this;
    }

    @Union({})
    public String getRawCountryCode() {
        return rawCountryCode;
    }

    @Union({})
    public MatchHistory setRawCountryCode(String rawCountryCode) {
        this.rawCountryCode = rawCountryCode;
        return this;
    }

    @Union({})
    public String getStandardisedCountryCode() {
        return standardisedCountryCode;
    }

    @Union({})
    public MatchHistory setStandardisedCountryCode(String standardisedCountryCode) {
        this.standardisedCountryCode = standardisedCountryCode;
        return this;
    }
    @Union({})
    public EntityMatchHistory getEntityMatchHistory() {
        return entityMatchHistory;
    }

    @Union({})
    public MatchHistory setEntityMatchHistory(EntityMatchHistory entityMatchHistory) {
        this.entityMatchHistory = entityMatchHistory;
        return this;
    }

    public MatchHistory withDnBMatchResult(DnBMatchContext dnbMatchContext) {
        withDnbMatchedNameLocation(dnbMatchContext.getMatchedNameLocation());

        this.dnbMatchedDUNS = dnbMatchContext.getDuns();
        this.confidenceCode = dnbMatchContext.getConfidenceCode();
        this.matchGrade = dnbMatchContext.getMatchGrade() != null
                && dnbMatchContext.getMatchGrade().getRawCode() != null
                        ? dnbMatchContext.getMatchGrade().getRawCode() : null;
        if (this.matchGrade != null) {
            DnBMatchGrade dnbMatchGrade = new DnBMatchGrade(this.matchGrade);
            this.nameMatchGradeText = dnbMatchGrade.getNameCode();
            this.cityMatchGradeText = dnbMatchGrade.getCityCode();
            this.stateMatchGradeText = dnbMatchGrade.getStateCode();
            this.phoneMatchGradeText = dnbMatchGrade.getPhoneCode();
            this.postalCodeMatchGradeText = dnbMatchGrade.getZipCodeCode();
        }

        this.isDnBMatch = dnbMatchContext.getHitWhiteCache() || dnbMatchContext.isCalledRemoteDnB();
        this.hitWhiteCache = dnbMatchContext.getHitWhiteCache();
        this.hitBlackCache = dnbMatchContext.getHitBlackCache();
        this.remoteDnbAPICall = dnbMatchContext.isCalledRemoteDnB();
        this.remoteDnbAPIRequestTime = DateTimeUtils.format(dnbMatchContext.getRequestTime());
        this.remoteDnbAPIResponseTime = DateTimeUtils.format(dnbMatchContext.getResponseTime());
        if (dnbMatchContext.getDnbCode() != null) {
            this.dnbReturnCode = dnbMatchContext.getDnbCode().toString();
        }
        this.passedAcceptanceCriteria = dnbMatchContext.isPassAcceptanceCriteria();

        return this;
    }

    public MatchHistory withRawNameLocation(NameLocation preMatchNameLocation) {
        if (preMatchNameLocation == null) {
            return this;
        }
        this.rawCompanyName = preMatchNameLocation.getName();
        this.rawCity = preMatchNameLocation.getCity();
        this.rawCountry = preMatchNameLocation.getCountry();
        this.rawCountryCode = preMatchNameLocation.getCountryCode();
        this.rawPhone = preMatchNameLocation.getPhoneNumber();
        this.rawState = preMatchNameLocation.getState();
        this.rawStreet = preMatchNameLocation.getStreet();
        this.rawPostalCode = preMatchNameLocation.getZipcode();
        return this;

    }

    public MatchHistory withStandardisedNameLocation(NameLocation parsedDomainLocation) {
        if (parsedDomainLocation == null) {
            return this;
        }
        this.standardisedCompanyName = parsedDomainLocation.getName();
        this.standardisedCity = parsedDomainLocation.getCity();
        this.standardisedCountry = parsedDomainLocation.getCountry();
        this.standardisedCountryCode = parsedDomainLocation.getCountryCode();
        this.standardisedPhone = parsedDomainLocation.getPhoneNumber();
        this.standardisedState = parsedDomainLocation.getState();
        this.standardisedStreet = parsedDomainLocation.getStreet();
        this.standardisedPostalCode = parsedDomainLocation.getZipcode();
        return this;
    }

    private MatchHistory withDnbMatchedNameLocation(NameLocation dnbMatchedNameLocation) {
        if (dnbMatchedNameLocation == null) {
            return this;
        }
        this.dnbMatchedCompanyName = dnbMatchedNameLocation.getName();
        this.dnbMatchedCity = dnbMatchedNameLocation.getCity();
        this.dnbMatchedCountry = dnbMatchedNameLocation.getCountry();
        this.dnbMatchedCountryCode = dnbMatchedNameLocation.getCountryCode();
        this.dnbMatchedPhone = dnbMatchedNameLocation.getPhoneNumber();
        this.dnbMatchedState = dnbMatchedNameLocation.getState();
        this.dnbMatchedStreet = dnbMatchedNameLocation.getStreet();
        this.dnbMatchedPostalCode = dnbMatchedNameLocation.getZipcode();
        return this;
    }

    public MatchHistory withMatchedNameLocation(NameLocation matchedNameLocation) {
        if (matchedNameLocation == null) {
            return this;
        }
        this.matchedCompanyName = matchedNameLocation.getName();
        this.matchedCity = matchedNameLocation.getCity();
        this.matchedCountry = matchedNameLocation.getCountry();
        this.matchedCountryCode = matchedNameLocation.getCountryCode();
        this.matchedPhone = matchedNameLocation.getPhoneNumber();
        this.matchedState = matchedNameLocation.getState();
        this.matchedStreet = matchedNameLocation.getStreet();
        this.matchedPostalCode = matchedNameLocation.getZipcode();
        return this;
    }

}
