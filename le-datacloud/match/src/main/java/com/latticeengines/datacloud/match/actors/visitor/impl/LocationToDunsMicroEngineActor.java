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
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("locationBasedMicroEngineActor")
@Scope("prototype")
public class LocationToDunsMicroEngineActor extends MicroEngineActorTemplate<DnbLookupActor> {
    private static final Log log = LogFactory.getLog(LocationToDunsMicroEngineActor.class);

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

        if (matchKeyTuple.getCountryCode() != null && matchKeyTuple.getName() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        if (response.getResult() != null) {
            MatchTraveler context = (MatchTraveler) response.getTravelerContext();
            MatchKeyTuple matchKeyTuple = context.getMatchKeyTuple();
            DnBMatchOutput res = (DnBMatchOutput) response.getResult();
            matchKeyTuple.setDuns(res.getDuns());
            if (res.getDnbCode() != DnBReturnCode.OK) {
                context.debug(getClass().getSimpleName() + " encountered issue with DnB for traveler " + context + ": "
                        + res.getDnbCode().getMessage());
            }
            response.setResult(null);
        }
    }
}
