package com.latticeengines.actors.visitor.sample.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.TravelerContext;
import com.latticeengines.actors.visitor.sample.SampleMicroEngineActorTemplate;

public class SampleDunsBasedMicroEngineActor extends SampleMicroEngineActorTemplate {
    private static final Log log = LogFactory.getLog(SampleDunsBasedMicroEngineActor.class);

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataSourceActor() {
        return "dynamo";
    }

    @Override
    protected boolean accept(TravelerContext traveler) {
        Map<String, Object> dataKeyValueMap = traveler.getDataKeyValueMap();

        if (dataKeyValueMap.containsKey("DUNS")) {
            return true;
        }

        return false;
    }

}
