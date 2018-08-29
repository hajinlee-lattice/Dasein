package com.latticeengines.domain.exposed.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class MatchKeyUtils {

    /**
     * Match key level -> accuracy level
     * Lower accuracy level, less information in match input
     */
    private static final Map<MatchKey, Integer> KEY_LEVEL = new HashMap<MatchKey, Integer>() {
        {
            put(MatchKey.Name, 0);
            put(MatchKey.Country, 1);
            put(MatchKey.State, 2);
            put(MatchKey.City, 3);
        }
    };

    /**
     * Only to evaluate name/location based match key level
     * Match key tuple without name should not be accepted by name/location based match
     * TODO: Evaluate key level for phone number and zipcode
     */
    public static MatchKey evalKeyLevel(MatchKeyTuple tuple) {
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode()) //
                && StringUtils.isNotBlank(tuple.getCity())) {
            return MatchKey.City;
        }
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode()) //
                && StringUtils.isNotBlank(tuple.getState())) {
            return MatchKey.State;
        }
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode())) {
            return MatchKey.Country;
        }
        if (StringUtils.isNotBlank(tuple.getName())) {
            return MatchKey.Name;
        }
        return null;
    }

    /**
     * Compare accuracy of match key level 
     * Lower accuracy level, less information in match input
     * Return 0: same key level
     * Return -1: compared has lower accuracy than compareTo
     * Return 1: compared has higher accuracy than compareTo
     */
    public static int compareKeyLevel(MatchKey compared, MatchKey compareTo) {
        if (!KEY_LEVEL.containsKey(compared) || !KEY_LEVEL.containsKey(compareTo)) {
            throw new UnsupportedOperationException(
                    String.format("Not able to compare match key level between %s and %s", compared, compareTo));
        }
        if (KEY_LEVEL.get(compared) > KEY_LEVEL.get(compareTo)) {
            return 1;
        }
        if (KEY_LEVEL.get(compared) < KEY_LEVEL.get(compareTo)) {
            return -1;
        }
        return 0;
    }
}
