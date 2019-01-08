package com.latticeengines.datacloud.match.actors.visitor;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import org.apache.commons.lang3.StringUtils;

/*
 * NOTE: Requires knowledge of actors, therefore put in the actor package and not outside, should only be used
 * by actors
 */
public class DnBMatchUtils {

    /**
     * check whether DnB match result attached in the traveler is from cache
     * @param traveler input traveler
     * @return true if yes, false otherwise
     */
    public static boolean isMatchResultFromCache(@NotNull MatchTraveler traveler) {
        DnBMatchContext context = getContext(traveler);
        return context != null && (context.getHitBlackCache() || context.getHitWhiteCache());
    }

    /**
     * check whether DnB match result attached in the traveler is from remote DnB API
     * @param traveler input traveler
     * @return true if yes, false otherwise
     */
    public static boolean isMatchResultFromRemote(@NotNull MatchTraveler traveler) {
        DnBMatchContext context = getContext(traveler);
        return context != null && context.isCalledRemoteDnB();
    }

    /**
     * retrieve cached DnB match result attached in the traveler
     * @param traveler input traveler
     * @return cached DnBMatchContext object, null if not found
     */
    public static DnBMatchContext getCacheResult(@NotNull MatchTraveler traveler) {
        if (!isMatchResultFromCache(traveler)) {
            return null;
        }
        return getContext(traveler);
    }

    /**
     * retrieve remote DnB match result attached in the traveler
     * @param traveler input traveler
     * @return DnBMatchContext object from remote API, null if not found
     */
    public static DnBMatchContext getRemoteResult(@NotNull MatchTraveler traveler) {
        if (!isMatchResultFromRemote(traveler)) {
            return null;
        }
        return getContext(traveler);
    }

    /**
     * Helper to retrieve parent DUNS from {@link DnBMatchContext}. The rule to determine parent DUNS is listed below:
     * (a) If DUDuns is not empty, return DUDuns
     * (b) Else if GUDuns is not empty, return GUDuns
     * (c) Else return Duns
     *
     * @param context target dnb match context
     * @return parent DUNS
     */
    public static String getFinalParentDuns(DnBMatchContext context) {
        if (context == null) {
            return null;
        }

        if (StringUtils.isNotBlank(context.getFinalDuDuns())) {
            return context.getFinalDuDuns();
        } else if (StringUtils.isNotBlank(context.getFinalGuDuns())) {
            return context.getFinalGuDuns();
        } else {
            return context.getFinalDuns();
        }
    }

    /*
     * retrieve DnB match result that matches the DUNS in the match key, null if no match found
     */
    private static DnBMatchContext getContext(@NotNull MatchTraveler traveler) {
        Preconditions.checkNotNull(traveler);
        String duns = getDuns(traveler);
        if (StringUtils.isBlank(duns) || traveler.getDnBMatchContexts() == null) {
            return null;
        }

        for (DnBMatchContext context : traveler.getDnBMatchContexts()) {
            if (context != null && duns.equals(context.getDuns())) {
                return context;
            }
        }
        return null;
    }

    /*
     * retrieve DUNS in the match key
     */
    private static String getDuns(@NotNull MatchTraveler traveler) {
        if (traveler.getMatchKeyTuple() == null) {
            return null;
        }
        return traveler.getMatchKeyTuple().getDuns();
    }
}
