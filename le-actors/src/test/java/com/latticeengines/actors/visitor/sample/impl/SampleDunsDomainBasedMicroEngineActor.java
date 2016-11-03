package com.latticeengines.actors.visitor.sample.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.sample.SampleMatchKeyTuple;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.SampleMicroEngineActorTemplate;

@Component("sampleDunsDomainBasedMicroEngineActor")
@Scope("prototype")
public class SampleDunsDomainBasedMicroEngineActor extends SampleMicroEngineActorTemplate<SampleDynamoLookupActor> {

    @Override
    protected Class<SampleDynamoLookupActor> getDataSourceActorClz() {
        return SampleDynamoLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        SampleMatchKeyTuple matchKeyTuple = ((SampleMatchTravelContext) traveler).getMatchKeyTuple();

        if (matchKeyTuple.getDomain() != null && matchKeyTuple.getDuns() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        SampleMatchTravelContext context = (SampleMatchTravelContext) response.getTravelerContext();
        if (response.getResult() != null) {
            context.setResult(response.getResult());
            context.setProcessed(true);
        } else {
            context.setProcessed(false);
        }
    }
}
