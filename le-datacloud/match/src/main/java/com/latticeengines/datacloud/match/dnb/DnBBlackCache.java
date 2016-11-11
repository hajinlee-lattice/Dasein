package com.latticeengines.datacloud.match.dnb;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchCache;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBBlackCache extends MatchCache<DnBBlackCache> {

    private static final String NAME_TOKEN = "_NAME_";
    private static final String COUNTRY_CODE_TOKEN = "_COUNTRYCODE_";
    private static final String STATE_TOKEN = "_STATE_";
    private static final String CITY_TOKEN = "_CITY_";
    private static final String ZIPCODE_TOKEN = "_ZIPCODE_";
    private static final String PHONE_TOKEN = "_PHONE_";
    private static final String EMAIL_TOKEN = "_EMAIL_";

    @Override
    public DnBBlackCache getInstance() {
        return this;
    }

    public DnBBlackCache() {

    }

    public DnBBlackCache(NameLocation nameLocation, String email) {
        getKeyTokenValues().put(NAME_TOKEN, nameLocation.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, nameLocation.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, nameLocation.getState());
        getKeyTokenValues().put(CITY_TOKEN, nameLocation.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, nameLocation.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, nameLocation.getPhoneNumber());
        getKeyTokenValues().put(EMAIL_TOKEN, email);
        buildId();
    }

    public DnBBlackCache(MatchKeyTuple matchKeyTuple) {
        getKeyTokenValues().put(NAME_TOKEN, matchKeyTuple.getName());
        getKeyTokenValues().put(COUNTRY_CODE_TOKEN, matchKeyTuple.getCountryCode());
        getKeyTokenValues().put(STATE_TOKEN, matchKeyTuple.getState());
        getKeyTokenValues().put(CITY_TOKEN, matchKeyTuple.getCity());
        getKeyTokenValues().put(ZIPCODE_TOKEN, matchKeyTuple.getZipcode());
        getKeyTokenValues().put(PHONE_TOKEN, matchKeyTuple.getPhoneNumber());
        getKeyTokenValues().put(EMAIL_TOKEN, matchKeyTuple.getEmail());
        buildId();
    }

}
