package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

@Component("domainBasedMicroEngineActor")
@Scope("prototype")
public class DomainBasedMicroEngineActor extends MicroEngineActorTemplate<DynamoLookupActor> {

    @Override
    protected Class<DynamoLookupActor> getDataSourceActorClz() {
        return DynamoLookupActor.class;
    }

    @Override
    protected boolean accept(TravelContext traveler) {

        MatchKeyTuple matchKeyTuple = ((MatchTravelContext) traveler).getMatchKeyTuple();

        if (matchKeyTuple.getDomain() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        MatchTravelContext context = (MatchTravelContext) response.getTravelerContext();
        if (response.getResult() != null) {
            context.setResult(response.getResult());
            context.setProcessed(true);
        } else {
            context.setProcessed(false);
        }
    }
}
