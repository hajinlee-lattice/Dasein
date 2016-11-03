package com.latticeengines.actors.visitor.sample.impl;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.sample.SampleMatchKeyTuple;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.SampleMicroEngineActorTemplate;

@Component("sampleLocationBasedMicroEngineActor")
@Scope("prototype")
public class SampleLocationToDunsMicroEngineActor extends SampleMicroEngineActorTemplate<SampleDnbLookupActor> {

    @Override
    protected Class<SampleDnbLookupActor> getDataSourceActorClz() {
        return SampleDnbLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        SampleMatchKeyTuple matchKeyTuple = ((SampleMatchTravelContext) traveler).getMatchKeyTuple();

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
            SampleMatchTravelContext context = (SampleMatchTravelContext) response.getTravelerContext();
            SampleMatchKeyTuple matchKeyTuple = context.getMatchKeyTuple();

            matchKeyTuple.setDuns((String) response.getResult());
            response.setResult(null);
        }
    }
}
