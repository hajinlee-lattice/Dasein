package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DnBMatchUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;
import com.latticeengines.datacloud.match.service.DnBMatchPostProcessor;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/*
 * validate remote DnB match result and perform post processing on the result (including checking if matched DUNS
 * exists in AM and update cache entry if necessary)
 */
@Component("dunsValidateMicroEngineActor")
@Scope("prototype")
public class DunsValidateMicroEngineActor extends MicroEngineActorTemplate<DynamoLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(DunsValidateMicroEngineActor.class);

    @Inject
    private DnBMatchPostProcessor dnBMatchPostProcessor;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setDuns(input.getDuns());
        return tuple;
    }

    @Override
    protected Class<DynamoLookupActor> getDataSourceActorClz() {
        return DynamoLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        MatchKeyTuple matchKeyTuple = matchTraveler.getMatchKeyTuple();
        MatchInput input = matchTraveler.getMatchInput();
        DnBMatchContext context = DnBMatchUtils.getRemoteResult(matchTraveler);
        return !alreadyValidated(matchTraveler)
                && dnBMatchPostProcessor.shouldPostProcessRemoteResult(context, input, matchKeyTuple);
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        DnBMatchContext context = DnBMatchUtils.getRemoteResult(traveler);

        // indicate that we already validate DnBMatchContext
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(getClass().getName(), context == null ? null : context.getDuns());

        boolean isDunsInAM = response.getResult() != null;
        boolean isResultValid = dnBMatchPostProcessor.postProcessRemoteResult(
                traveler, context, traveler.getDunsOriginMap(), isDunsInAM);
        if (!isResultValid) {
            traveler.debug(String.format(
                    "Invalid remote DnBMatchContext, DUNS=%s DnBCode=%s CurrentIsDunsInAM=%s",
                    tuple.getDuns(), context.getDnbCodeAsString(), isDunsInAM));
            tuple.setDuns(null);
        }
    }

    private boolean alreadyValidated(@NotNull MatchTraveler traveler) {
        if (traveler.getDunsOriginMap() == null) {
            return false;
        }

        return traveler.getDunsOriginMap().containsKey(getClass().getName());
    }
}
