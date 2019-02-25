package com.latticeengines.datacloud.match.service;

import java.util.Map;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public interface DnBMatchPostProcessor {

    /**
     * Determine whether we need to post process cached DnB match result
     * @param context cached DnBMatch result, nullable
     * @param input match input provided by the user, nullable
     * @param tuple match key provided by the user, nullable
     * @return true if we need to post process the result, call postProcessCacheResult next
     */
    boolean shouldPostProcessCacheResult(DnBMatchContext context, MatchInput input, MatchKeyTuple tuple);

    /**
     * Determine whether we need to post process remote DnB match result
     * @param context remote DnBMatch result, nullable
     * @param input match input provided by the user, nullable
     * @param tuple match key provided by the user, nullable
     * @return true if we need to post process the result, call postProcessRemoteResult next
     */
    boolean shouldPostProcessRemoteResult(DnBMatchContext context, MatchInput input, MatchKeyTuple tuple);

    /**
     * Post process cached DnB match result, should only be called when shouldPostProcessCacheResult is true
     * @param traveler traveler object
     * @param context cached DnBMatch result, should not be null
     * @param dunsOriginMap map to track where DUNS come from
     * @param currentIsDunsInAM flag to indicate whether matched DUNS is current in account master
     * @return true if the match result is valid, false otherwise
     */
    boolean postProcessCacheResult(
            @NotNull Traveler traveler, @NotNull DnBMatchContext context,
            @NotNull Map<String, String> dunsOriginMap, boolean currentIsDunsInAM);

    /**
     * Post process remote DnB match result, should only be called when shouldPostProcessRemoteResult is true
     * @param traveler traveler object
     * @param context cached DnBMatch result, should not be null
     * @param dunsOriginMap map to track where DUNS come from
     * @param currentIsDunsInAM flag to indicate whether matched DUNS is current in account master
     * @return true if the match result is valid, false otherwise
     */
    boolean postProcessRemoteResult(
            @NotNull Traveler traveler, @NotNull DnBMatchContext context,
            @NotNull Map<String, String> dunsOriginMap, boolean currentIsDunsInAM);
}
