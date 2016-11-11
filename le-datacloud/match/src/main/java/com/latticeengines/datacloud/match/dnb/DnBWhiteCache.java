package com.latticeengines.datacloud.match.dnb;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchCache;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBWhiteCache extends MatchCache<DnBWhiteCache> {

    private static final String DUNS = "Duns";
    private static final String CONFIDENCE_CODE = "ConfidenceCode";
    private static final String MATCH_GRADE = "MatchGrade";

    private static final String NAME_TOKEN = "_NAME_";
    private static final String COUNTRY_CODE_TOKEN = "_COUNTRYCODE_";
    private static final String STATE_TOKEN = "_STATE_";
    private static final String CITY_TOKEN = "_CITY_";
    private static final String ZIPCODE_TOKEN = "_ZIPCODE_";
    private static final String PHONE_TOKEN = "_PHONE_";
    private static final String EMAIL_TOKEN = "_EMAIL_";

    private String duns;

    private Integer confidenceCode;

    private DnBMatchGrade matchGrade;

    @Override
    public DnBWhiteCache getInstance() {
        return this;
    }

    public DnBWhiteCache() {

    }

    public DnBWhiteCache(NameLocation nameLocation, String email, String duns, Integer confidenceCode,
            DnBMatchGrade matchGrade) {
        getKeyTokenValues().put(NAME_TOKEN, nameLocation.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, nameLocation.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, nameLocation.getState());
        getKeyTokenValues().put(CITY_TOKEN, nameLocation.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, nameLocation.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, nameLocation.getPhoneNumber());
        getKeyTokenValues().put(EMAIL_TOKEN, email);
        buildId();
        Map<String, Object> cacheContext = new HashMap<String, Object>();
        cacheContext.put(DUNS, duns);
        cacheContext.put(CONFIDENCE_CODE, confidenceCode);
        cacheContext.put(MATCH_GRADE, matchGrade.getRawCode());
        setCacheContext(cacheContext);
    }

    public DnBWhiteCache(MatchKeyTuple matchKeyTuple) {
        getKeyTokenValues().put(NAME_TOKEN, matchKeyTuple.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, matchKeyTuple.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, matchKeyTuple.getState());
        getKeyTokenValues().put(CITY_TOKEN, matchKeyTuple.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, matchKeyTuple.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, matchKeyTuple.getPhoneNumber());
        getKeyTokenValues().put(EMAIL_TOKEN, matchKeyTuple.getEmail());
        buildId();
    }

    public void parseCacheContext() {
        duns = getCacheContext().containsKey(DUNS) ? (String) getCacheContext().get(DUNS) : null;
        confidenceCode = getCacheContext().containsKey(CONFIDENCE_CODE)
                ? (Integer) getCacheContext().get(CONFIDENCE_CODE) : null;
        matchGrade = getCacheContext().containsKey(MATCH_GRADE)
                ? new DnBMatchGrade((String) getCacheContext().get(MATCH_GRADE)) : null;
    }

    public String getDuns() {
        return duns;
    }

    public Integer getConfidenceCode() {
        return confidenceCode;
    }

    public DnBMatchGrade getMatchGrade() {
        return matchGrade;
    }

}
