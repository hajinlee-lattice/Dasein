package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

@Component("dunsBasedMicroEngineActor")
@Scope("prototype")
public class DunsBasedMicroEngineActor extends MicroEngineActorTemplate {
    @Override
    protected String getDataSourceActor() {
        return "dynamoLookupActor";
    }

    @Override
    protected boolean accept(TravelContext traveler) {
        Map<String, Object> dataKeyValueMap = traveler.getDataKeyValueMap();

        if (dataKeyValueMap.containsKey("DUNS")) {
            return true;
        }

        return false;
    }
}
