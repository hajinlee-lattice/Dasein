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
 * validate remote DnB match result and perform post processing on the result (including checking if matched DUNS
 * exists in AM and refresh the cache if necessary).
 *
 * after the validation, DUNS redirection is performed.
 */
@Component("dunsGuideValidateMicroEngineActor")
@Scope("prototype")
public class DunsGuideValidateMicroEngineActor extends BaseDunsGuideValidateMicroEngineActor {
    private static final Logger log = LoggerFactory.getLogger(DunsGuideValidateMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
        traveler.addEntityLdcMatchTypeToTupleList(
                Pair.of(EntityMatchType.LDC_DUNS_GUIDE_VALIDATE, traveler.getMatchKeyTuple()));
    }

    @Override
    protected boolean postProcessDnBMatchResult(@NotNull MatchTraveler traveler, boolean isDunsInAM) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        MatchInput input = traveler.getMatchInput();
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        DnBMatchContext remoteContext = DnBMatchUtils.getRemoteResult(traveler);
        boolean isResultValid = true;
        if (dnBMatchPostProcessor.shouldPostProcessRemoteResult(remoteContext, input, tuple)) {
            isResultValid = dnBMatchPostProcessor.postProcessRemoteResult(
                    traveler, remoteContext, traveler.getDunsOriginMap(), isDunsInAM);
            if (!isResultValid) {
                String dnbCodeStr = remoteContext == null ? null : remoteContext.getDnbCodeAsString();
                traveler.debug(String.format(
                        "Invalid remote DnBMatchContext, DUNS=%s DnBCode=%s CurrentIsDunsInAM=%s",
                        tuple.getDuns(), dnbCodeStr, isDunsInAM));
            }
        }
        return isResultValid;
    }
}
