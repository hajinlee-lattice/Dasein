package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
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
        Map<String, Object> dataKeyValueMap = traveler.getDataKeyValueMap();

        if (dataKeyValueMap.containsKey("CompanyName") //
                && dataKeyValueMap.containsKey("Country")//
                && (dataKeyValueMap.containsKey("City") //
                        || dataKeyValueMap.containsKey("State"))) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        if (response.getResult() != null) {
            Map<String, Object> dataMap = response.getTravelerContext().getDataKeyValueMap();
            dataMap.put("DUNS", response.getResult());
            response.setResult(null);
        }
    }
}
