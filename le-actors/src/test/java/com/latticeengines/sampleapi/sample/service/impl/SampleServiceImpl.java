//package com.latticeengines.sampleapi.sample.service.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.actors.exposed.traveler.GuideBook;
//import com.latticeengines.actors.visitor.sample.SampleMatchActorSystemWrapper;
//import com.latticeengines.actors.visitor.sample.SampleMatchTravelerContext;
//import com.latticeengines.sampleapi.sample.service.SampleService;
//
//import akka.pattern.Patterns;
//import akka.util.Timeout;
//import scala.concurrent.Await;
//import scala.concurrent.Future;
//import scala.concurrent.duration.FiniteDuration;
//
//@Component
//public class SampleServiceImpl implements SampleService {
//    private static final Log log = LogFactory.getLog(SampleServiceImpl.class);
//
//    @Autowired
//    private SampleMatchActorSystemWrapper wrapper;
//
//    @Override
//    public Object callMatch(Map<String, Object> matchRequest) throws Exception {
//        List<Map<String, Object>> matchRequests = new ArrayList<>();
//        matchRequests.add(matchRequest);
//        return callMatch(matchRequests).get(0);
//    }
//
//    @Override
//    public List<Object> callMatch(List<Map<String, Object>> matchRequests) throws Exception {
//        List<Object> results = new ArrayList<>();
//
//        FiniteDuration duration = new FiniteDuration(10, TimeUnit.MINUTES);
//        Timeout timeout = new Timeout(duration);
//        List<Future<Object>> matchFutures = new ArrayList<>();
//        for (Map<String, Object> matchRequest : matchRequests) {
//            GuideBook gb = wrapper.createGuideBook();
//            SampleMatchTravelerContext traveler = new SampleMatchTravelerContext(UUID.randomUUID().toString(), gb);
//
//            traveler.setDataKeyValueMap(matchRequest);
//
//            matchFutures.add(askFuzzyMatchAnchor(traveler, timeout));
//        }
//
//        for (Future<Object> future : matchFutures) {
//            Object result = (Object) Await.result(future, timeout.duration());
//            log.info("Got result: " + result);
//            results.add(result);
//        }
//        return results;
//    }
//
//    private Future<Object> askFuzzyMatchAnchor(SampleMatchTravelerContext traveler, Timeout timeout) {
//        return Patterns.ask(wrapper.getFuzzyMatchAnchor(), traveler, timeout);
//    }
//
//}
