package com.latticeengines.sampleapi.sample.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.visitor.sample.SampleActorSystemWrapper;
import com.latticeengines.actors.visitor.sample.SampleTraveler;
import com.latticeengines.sampleapi.sample.service.SampleService;

import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class SampleServiceImpl implements SampleService {
    private static final Log log = LogFactory.getLog(SampleServiceImpl.class);

    @Autowired
    private SampleActorSystemWrapper wrapper;

    @Override
    public Object doSampleWork(Object matchRequest) throws Exception {
        List<Object> matchRequests = new ArrayList<>();
        matchRequests.add(matchRequest);
        return doSampleWork(matchRequests).get(0);
    }

    @Override
    public List<Object> doSampleWork(List<Object> matchRequests) throws Exception {
        List<Object> results = new ArrayList<>();

        FiniteDuration duration = new FiniteDuration(10, TimeUnit.MINUTES);
        Timeout timeout = new Timeout(duration);
        List<Future<Object>> matchFutures = new ArrayList<>();
        for (Object matchRequest : matchRequests) {
            GuideBook gb = wrapper.createGuideBook();
            SampleTraveler traveler = new SampleTraveler(UUID.randomUUID().toString(), gb);
            traveler.setData(matchRequest);

            matchFutures.add(askSampleActor(traveler, timeout));
        }

        for (Future<Object> future : matchFutures) {
            SampleTraveler result = (SampleTraveler) Await.result(future, timeout.duration());
            log.info("Got result: " + result.getData());
            results.add(result);
        }
        return results;
    }

    private Future<Object> askSampleActor(SampleTraveler traveler, Timeout timeout) {
        return Patterns.ask(wrapper.getSampleActor(), traveler, timeout);
    }

}
