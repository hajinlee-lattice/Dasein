package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

@Component("locationBasedMicroEngineActor")
@Scope("prototype")
public class LocationToDunsMicroEngineActor extends MicroEngineActorTemplate<DnbLookupActor> {

    @Override
    protected Class<DnbLookupActor> getDataSourceActorClz() {
        return DnbLookupActor.class;
    }

    @Override
    protected boolean accept(TravelContext traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTravelContext) traveler).getMatchKeyTuple();

        if ((matchKeyTuple.getCity() != null //
                || matchKeyTuple.getState() != null) //
                && matchKeyTuple.getCountry() != null//
                && matchKeyTuple.getName() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        if (response.getResult() != null) {
            MatchTravelContext context = (MatchTravelContext) response.getTravelerContext();
            MatchKeyTuple matchKeyTuple = context.getMatchKeyTuple();

            matchKeyTuple.setDuns((String) response.getResult());
            response.setResult(null);
        }
    }
}
