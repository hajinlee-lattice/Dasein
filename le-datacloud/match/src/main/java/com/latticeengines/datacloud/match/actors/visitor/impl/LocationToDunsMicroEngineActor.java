package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;

@Component("locationBasedMicroEngineActor")
@Scope("prototype")
public class LocationToDunsMicroEngineActor extends MicroEngineActorTemplate<DnbLookupActor> {
    private static final Log log = LogFactory.getLog(LocationToDunsMicroEngineActor.class);

    private static final String HIT_WHITE_CACHE = "Retrieved a DUNS from white cache. Did not go to remote DnB API.";
    private static final String HIT_BLACK_CACHE = "Fast failed by black cache. Did not go to remote DnB API.";
    private static final String HIT_NO_CACHE = "Did not hit either white or black cache. Went to remote DnB API.";

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<DnbLookupActor> getDataSourceActorClz() {
        return DnbLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();

        if (matchKeyTuple.getDuns() != null) {
            return false;
        }

        if (matchKeyTuple.getCountryCode() != null && matchKeyTuple.getName() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        if (response.getResult() != null) {
            MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
            MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
            DnBMatchContext res = (DnBMatchContext) response.getResult();
            traveler.debug(res.getHitWhiteCache() ? HIT_WHITE_CACHE
                    : (res.getHitBlackCache() ? HIT_BLACK_CACHE : HIT_NO_CACHE));
            String suffix = String.format("ConfidenceCode = %s, MatchGrade = %s.",
                    (res.getConfidenceCode() == null ? "null" : res.getConfidenceCode().toString()),
                    (res.getMatchGrade() == null ? "null" : res.getMatchGrade().getRawCode()));
            if (res.getDuns() != null) {
                traveler.debug(
                        String.format("Found DUNS=%s at %s. %s", res.getDuns(), getClass().getSimpleName(), suffix));
            }
            matchKeyTuple.setDuns(res.getDuns());
            if (res.getDnbCode() != DnBReturnCode.OK) {
                traveler.debug(String.format("%s encountered an issue with DUNS lookup: %s. %s", //
                        getClass().getSimpleName(), //
                        (res.getDnbCode() == null ? "No DnBReturnCode" : res.getDnbCode().getMessage()), suffix));
            }
            traveler.getDnBMatchContexts().add(res);
            response.setResult(null);
        }
    }
}
