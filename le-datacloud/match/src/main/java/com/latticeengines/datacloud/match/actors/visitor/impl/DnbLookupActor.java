package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.TimerMessage;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private static final Log log = LogFactory.getLog(DnbLookupActor.class);

    @PostConstruct
    public void postConstruct() {
        TimerMessage timerMessage = new TimerMessage(DnbLookupActor.class);
        matchActorSystem.registerTimer(DnbLookupActor.class, 5, TimeUnit.MINUTES, timerMessage);
        log.info("Registered for timer call");
    }

    @Autowired
    @Qualifier("dnBLookupService")
    private DataSourceLookupService dnBLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }

    @Override
    protected void processTimerMessage(TimerMessage msg) {
        // handle timer message
        log.debug("Got timer call");
    }
    
    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
