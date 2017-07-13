package com.latticeengines.actors.visitor.sample.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.TimerMessage;
import com.latticeengines.actors.exposed.TimerRegistrationHelper;
import com.latticeengines.actors.exposed.TimerRegistrationRequest;
import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
import com.latticeengines.actors.visitor.sample.SampleDataSourceWrapperActorTemplate;

@Component("sampleDnbLookupActor")
@Scope("prototype")
public class SampleDnbLookupActor extends SampleDataSourceWrapperActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(SampleDnbLookupActor.class);

    @Autowired
    @Qualifier("sampleDnBLookupService")
    private SampleDataSourceLookupService dnBLookupService;

    public static TimerRegistrationHelper timerRegistrationHelper = new TimerRegistrationHelper(
            SampleDnbLookupActor.class);

    public static AtomicInteger actorCardinalityCounterForTest = new AtomicInteger(0);
    public static AtomicInteger timerCallCounterForTest = new AtomicInteger(0);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
        actorCardinalityCounterForTest.incrementAndGet();
        TimerRegistrationRequest request = new TimerRegistrationRequest(1, TimeUnit.SECONDS, null);

        timerRegistrationHelper.register(//
                context().system(), //
                sampleMatchActorSystem.getActorRef(SampleDnbLookupActor.class), //
                request);
    }

    @Override
    protected void processTimerMessage(TimerMessage msg) {
        // handle timer message
        timerCallCounterForTest.incrementAndGet();
    }

    @Override
    protected SampleDataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }
}
