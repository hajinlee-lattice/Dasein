package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelerContext;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

public class LocationBasedMicroEngineActor extends MicroEngineActorTemplate {
    private static final Log log = LogFactory.getLog(LocationBasedMicroEngineActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataSourceActor() {
        return "dnb";
    }

    @Override
    protected boolean accept(TravelerContext traveler) {
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
