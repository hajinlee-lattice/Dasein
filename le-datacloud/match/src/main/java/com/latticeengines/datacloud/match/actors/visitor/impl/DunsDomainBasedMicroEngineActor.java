package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.datacloud.match.actors.visitor.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;

public class DunsDomainBasedMicroEngineActor extends MicroEngineActorTemplate {
    private static final Log log = LogFactory.getLog(DunsDomainBasedMicroEngineActor.class);
    private MatchGuideBook guideBook;
    private String dataSourceActor;

    public DunsDomainBasedMicroEngineActor(MatchGuideBook guideBook, String dataSourceActor) {
        super();
        this.guideBook = guideBook;
        this.dataSourceActor = dataSourceActor;
    }

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataSourceActor() {
        return dataSourceActor;
    }

    @Override
    protected boolean accept(TravelContext traveler) {
        Map<String, Object> dataKeyValueMap = traveler.getDataKeyValueMap();

        if (dataKeyValueMap.containsKey("DUNS") //
                && dataKeyValueMap.containsKey("Domain")) {
            return true;
        }

        return false;
    }

}
