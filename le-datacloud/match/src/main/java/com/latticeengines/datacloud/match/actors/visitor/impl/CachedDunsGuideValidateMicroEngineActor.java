package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DnBMatchUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/*
 * validate cached DnB match result and perform post processing on the result (including checking if matched DUNS
 * exists in AM and whether a DnB remote API lookup is required to refresh the cache).
 *
 * after the validation, DUNS redirection is performed.
 */
@Component("cachedDunsGuideValidateMicroEngineActor")
@Scope("prototype")
public class CachedDunsGuideValidateMicroEngineActor extends BaseDunsGuideValidateMicroEngineActor {
    private static final Logger log = LoggerFactory.getLogger(CachedDunsGuideValidateMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected boolean postProcessDnBMatchResult(@NotNull MatchTraveler traveler, boolean isDunsInAM) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();

        // $JAW$
        traveler.addEntityLdcMatchTypeToTupleList(Pair.of(EntityMatchType.LDC_CACHED_DUNS_GUIDE_VALIDATE, tuple));

        MatchInput input = traveler.getMatchInput();
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        DnBMatchContext cacheContext = DnBMatchUtils.getCacheResult(traveler);
        boolean isResultValid = true;
        if (dnBMatchPostProcessor.shouldPostProcessCacheResult(cacheContext, input, tuple)) {
            isResultValid = dnBMatchPostProcessor.postProcessCacheResult(
                    traveler, cacheContext, traveler.getDunsOriginMap(), isDunsInAM);
            if (!isResultValid) {
                String cacheId = cacheContext == null ? null : cacheContext.getCacheId();
                traveler.debug(String.format(
                        "Invalid cached DnBMatchContext, CacheId=%s DUNS=%s DnBCode=%s CurrentIsDunsInAM=%s",
                        cacheId, tuple.getDuns(), cacheContext.getDnbCodeAsString(), isDunsInAM));
            }
        }
        return isResultValid;
    }
}
