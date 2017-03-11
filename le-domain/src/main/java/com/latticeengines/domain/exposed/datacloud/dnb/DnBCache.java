package com.latticeengines.domain.exposed.datacloud.dnb;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.datacloud.match.MatchCache;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;

public class DnBCache extends MatchCache<DnBCache> {

    private static final String DUNS = "Duns";
    private static final String CONFIDENCE_CODE = "ConfidenceCode";
    private static final String MATCH_GRADE = "MatchGrade";
    private static final String NAME_LOCATION = "NameLocation";
    private static final String OUT_OF_BUSINESS = "OutOfBusiness";
    private static final String DUNS_IN_AM = "DunsInAM";

    private static final String NAME_TOKEN = "_NAME_";
    private static final String COUNTRY_CODE_TOKEN = "_COUNTRYCODE_";
    private static final String STATE_TOKEN = "_STATE_";
    private static final String CITY_TOKEN = "_CITY_";
    private static final String ZIPCODE_TOKEN = "_ZIPCODE_";
    private static final String PHONE_TOKEN = "_PHONE_";
    private static final String EMAIL_TOKEN = "_EMAIL_";

    public static final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;

    private String duns;

    private Integer confidenceCode;

    private DnBMatchGrade matchGrade;

    private NameLocation matchedNameLocation;

    private Boolean outOfBusiness;

    private Boolean dunsInAM;

    // Identify it is white cache or black cache
    private boolean whiteCache;

    @DynamoAttribute(TIMESTAMP)
    private Long timestamp;

    @Override
    public DnBCache getInstance() {
        return this;
    }

    public DnBCache() {}

    // For cache lookup and black cache write
    public DnBCache(NameLocation nameLocation) {
        getKeyTokenValues().put(NAME_TOKEN, nameLocation.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, nameLocation.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, nameLocation.getState());
        getKeyTokenValues().put(CITY_TOKEN, nameLocation.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, nameLocation.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, nameLocation.getPhoneNumber());
        buildId();
        setTimestamp(System.currentTimeMillis() / DAY_IN_MILLIS);
    }

    // For cache lookup and black cache write
    public DnBCache(String email) {
        getKeyTokenValues().put(EMAIL_TOKEN, email);
        buildId();
        setTimestamp(System.currentTimeMillis() / DAY_IN_MILLIS);
    }

    // For white cache write
    public DnBCache(NameLocation nameLocation, String duns, Integer confidenceCode, DnBMatchGrade matchGrade,
            NameLocation matchedNameLocation, Boolean outOfBusiness, Boolean dunsInAM) {
        getKeyTokenValues().put(NAME_TOKEN, nameLocation.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, nameLocation.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, nameLocation.getState());
        getKeyTokenValues().put(CITY_TOKEN, nameLocation.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, nameLocation.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, nameLocation.getPhoneNumber());
        buildId();
        Map<String, Object> cacheContext = new HashMap<String, Object>();
        cacheContext.put(DUNS, duns);
        cacheContext.put(CONFIDENCE_CODE, confidenceCode);
        cacheContext.put(MATCH_GRADE, matchGrade.getRawCode());
        cacheContext.put(NAME_LOCATION, matchedNameLocation);
        cacheContext.put(OUT_OF_BUSINESS, outOfBusiness);
        cacheContext.put(DUNS_IN_AM, dunsInAM);
        setCacheContext(cacheContext);
        setTimestamp(System.currentTimeMillis() / DAY_IN_MILLIS);
    }

    // For white cache write
    public DnBCache(String email, String duns) {
        getKeyTokenValues().put(EMAIL_TOKEN, email);
        buildId();
        Map<String, Object> cacheContext = new HashMap<String, Object>();
        cacheContext.put(DUNS, duns);
        setCacheContext(cacheContext);
        setTimestamp(System.currentTimeMillis() / DAY_IN_MILLIS);
    }

    public void parseCacheContext() {
        if (getCacheContext() == null) { // Black cache
            whiteCache = false;
            return;
        }
        // white cache
        duns = getCacheContext().containsKey(DUNS) ? (String) getCacheContext().get(DUNS) : null;
        confidenceCode = getCacheContext().containsKey(CONFIDENCE_CODE)
                ? (Integer) getCacheContext().get(CONFIDENCE_CODE) : null;
        matchGrade = getCacheContext().containsKey(MATCH_GRADE)
                ? new DnBMatchGrade((String) getCacheContext().get(MATCH_GRADE)) : null;
        if (getCacheContext().get(NAME_LOCATION) != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            matchedNameLocation = objectMapper.convertValue(getCacheContext().get(NAME_LOCATION), NameLocation.class);
        } else {
            matchedNameLocation = new NameLocation();
        }
        outOfBusiness = getCacheContext().containsKey(OUT_OF_BUSINESS)
                ? (Boolean) getCacheContext().get(OUT_OF_BUSINESS) : null;
        dunsInAM = getCacheContext().containsKey(DUNS_IN_AM) ? (Boolean) getCacheContext().get(DUNS_IN_AM) : null;
        whiteCache = true;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public Integer getConfidenceCode() {
        return confidenceCode;
    }

    public void setConfidenceCode(Integer confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    public DnBMatchGrade getMatchGrade() {
        return matchGrade;
    }

    public void setMatchGrade(DnBMatchGrade matchGrade) {
        this.matchGrade = matchGrade;
    }

    public NameLocation getMatchedNameLocation() {
        return matchedNameLocation;
    }

    public void setMatchedNameLocation(NameLocation matchedNameLocation) {
        this.matchedNameLocation = matchedNameLocation;
    }

    public boolean isWhiteCache() {
        return whiteCache;
    }

    public void setWhiteCache(boolean whiteCache) {
        this.whiteCache = whiteCache;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Boolean isOutOfBusiness() {
        return outOfBusiness;
    }

    public void setOutOfBusiness(Boolean outOfBusiness) {
        this.outOfBusiness = outOfBusiness;
    }

    public Boolean isDunsInAM() {
        return dunsInAM;
    }

    public void setDunsInAM(Boolean dunsInAM) {
        this.dunsInAM = dunsInAM;
    }

}
