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

@Component("sampleDynamoLookupActor")
@Scope("prototype")
public class SampleDynamoLookupActor extends SampleDataSourceWrapperActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(SampleDynamoLookupActor.class);

    @Autowired
    @Qualifier("sampleDynamoDBLookupService")
    private SampleDataSourceLookupService dynamoDBLookupService;

    public static TimerRegistrationHelper timerRegistrationHelper = new TimerRegistrationHelper(
            SampleDynamoLookupActor.class);

    public static AtomicInteger actorCardinalityCounterForTest = new AtomicInteger(0);
    public static AtomicInteger timerCallCounterForTest = new AtomicInteger(0);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
        actorCardinalityCounterForTest.incrementAndGet();
        TimerRegistrationRequest request = new TimerRegistrationRequest(50, TimeUnit.MILLISECONDS, null);

        timerRegistrationHelper.register(//
                context().system(), //
                sampleMatchActorSystem.getActorRef(SampleDynamoLookupActor.class), //
                request);
    }

    @Override
    protected SampleDataSourceLookupService getDataSourceLookupService() {
        return dynamoDBLookupService;
    }

    @Override
    protected void processTimerMessage(TimerMessage msg) {
        // handle timer message
        timerCallCounterForTest.incrementAndGet();
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
