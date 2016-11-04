package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.TimerMessage;
import com.latticeengines.actors.exposed.TimerRegistrationHelper;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private static final Log log = LogFactory.getLog(DnbLookupActor.class);

    public static TimerRegistrationHelper timerRegistrationHelper = new TimerRegistrationHelper(DnbLookupActor.class);

    @Value("${datacloud.match.dnbLookupActor.timer.frequency:5}")
    private int timerFrequency;

    @Value("${datacloud.match.dnbLookupActor.timer.frequency.unit:MINUTES}")
    private TimeUnit timerFrequencyUnit;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
        timerRegistrationHelper.register(//
                context().system(), //
                matchActorSystem.getActorRef(DnbLookupActor.class), //
                timerFrequency, timerFrequencyUnit);
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
