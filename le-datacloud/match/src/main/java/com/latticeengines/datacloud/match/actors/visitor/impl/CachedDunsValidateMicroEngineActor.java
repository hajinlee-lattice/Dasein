package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Collections;
import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.DnBMatchUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.DnBMatchPostProcessor;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/*
 * validate cached DnB match result and perform post processing on the result (including checking if matched DUNS
 * exists in AM and whether a DnB remote API lookup is required to refresh the cache)
 */
@Component("cachedDunsValidateMicroEngineActor")
@Scope("prototype")
public class CachedDunsValidateMicroEngineActor extends DataSourceMicroEngineTemplate<DynamoLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(CachedDunsValidateMicroEngineActor.class);

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
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        DnBMatchContext context = DnBMatchUtils.getCacheResult(traveler);
        MatchInput input = traveler.getMatchInput();
        return !alreadyValidated(traveler)
                && dnBMatchPostProcessor.shouldPostProcessCacheResult(context, input, matchKeyTuple);
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
        traveler.addEntityLdcMatchTypeToTupleList(
                Pair.of(LdcMatchType.CACHED_DUNS_VALIDATE, traveler.getMatchKeyTuple()));
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        DnBMatchContext context = DnBMatchUtils.getCacheResult(traveler);

        // indicate that we already validate DnBMatchContext
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(getClass().getName(), context == null ? null : context.getDuns());

        boolean isDunsInAM = response.getResult() != null;
        boolean isResultValid = dnBMatchPostProcessor.postProcessCacheResult(traveler, context,
                traveler.getDunsOriginMap(), isDunsInAM);
        if (!isResultValid) {
            traveler.debug(
                    String.format("Invalid cached DnBMatchContext, CacheId=%s DUNS=%s DnBCode=%s CurrentIsDunsInAM=%s",
                            context.getCacheId(), tuple.getDuns(), context.getDnbCodeAsString(), isDunsInAM));
            tuple.setDuns(null);
        }
        traveler.addEntityMatchLookupResults(BusinessEntity.LatticeAccount.name(),
                Collections.singletonList(Pair.of(traveler.getMatchKeyTuple(),
                        Collections.singletonList(tuple.getDuns()))));
    }

    private boolean alreadyValidated(@NotNull MatchTraveler traveler) {
        if (traveler.getDunsOriginMap() == null) {
            return false;
        }

        return traveler.getDunsOriginMap().containsKey(getClass().getName());
    }
}
