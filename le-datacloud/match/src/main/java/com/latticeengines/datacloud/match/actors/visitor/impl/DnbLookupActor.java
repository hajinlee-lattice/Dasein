package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
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
import com.latticeengines.actors.exposed.TimerRegistrationRequest;
import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private static final Log log = LogFactory.getLog(DnbLookupActor.class);

    public static TimerRegistrationHelper timerRegistrationHelper = new TimerRegistrationHelper(DnbLookupActor.class);

    @Value("${datacloud.match.dnbLookupActor.dispatcher.timer.frequency:5}")
    private int dispatcherTimerFrequency;

    @Value("${datacloud.match.dnbLookupActor.dispatcher.timer.frequency.unit:MINUTES}")
    private TimeUnit dispatcherTimerFrequencyUnit;

    @Value("${datacloud.match.dnbLookupActor.fetcher.timer.frequency:30}")
    private int fetcherTimerFrequency;

    @Value("${datacloud.match.dnbLookupActor.fetcher.timer.frequency.unit:SECONDS}")
    private TimeUnit fetcherTimerFrequencyUnit;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
        TimerMessage dispatcherTimerMessage = new TimerMessage(DnbLookupActor.class);
        dispatcherTimerMessage.setContext(BulkLookupStrategy.DISPATCHER);
        TimerRegistrationRequest dispatcherTimerRequest = new TimerRegistrationRequest(dispatcherTimerFrequency,
                dispatcherTimerFrequencyUnit, dispatcherTimerMessage);
        TimerMessage fetcherTimerMessage = new TimerMessage(DnbLookupActor.class);
        fetcherTimerMessage.setContext(BulkLookupStrategy.FETCHER);
        TimerRegistrationRequest fetcherTimerRequest = new TimerRegistrationRequest(fetcherTimerFrequency,
                fetcherTimerFrequencyUnit, fetcherTimerMessage);

        List<TimerRegistrationRequest> requests = new ArrayList<>();
        requests.add(dispatcherTimerRequest);
        requests.add(fetcherTimerRequest);

        timerRegistrationHelper.register(//
                context().system(), //
                matchActorSystem.getActorRef(DnbLookupActor.class), //
                requests);
    }

    @Autowired
    @Qualifier("dnBLookupService")
    private DataSourceLookupServiceBase dnBLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }

    @Override
    protected void processTimerMessage(TimerMessage msg) {
        // handle timer message
        if (log.isDebugEnabled()) {
            log.debug("Got timer call: " + msg.getContext().toString());
        }

        ExecutorService executor = matchActorSystem.getDataSourceServiceExecutor();
        Runnable task = createLookupRunnable((BulkLookupStrategy) msg.getContext());
        executor.execute(task);
    }

    private Runnable createLookupRunnable(final BulkLookupStrategy strategy) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                dnBLookupService.bulkLookup(strategy);
            }
        };
        return task;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
