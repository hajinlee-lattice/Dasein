//package com.latticeengines.datacloud.match.service.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.actors.exposed.traveler.GuideBook;
//import com.latticeengines.datacloud.match.actors.visitor.MatchActorSystemWrapper;
//import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
//import com.latticeengines.datacloud.match.service.FuzzyMatchService;
//
//import akka.pattern.Patterns;
//import akka.util.Timeout;
//import scala.concurrent.Await;
//import scala.concurrent.Future;
//import scala.concurrent.duration.FiniteDuration;
//
//@Component
//public class FuzzyMatchServiceImpl implements FuzzyMatchService {
//    private static final Log log = LogFactory.getLog(FuzzyMatchServiceImpl.class);
//
//    @Autowired
//    private MatchActorSystemWrapper wrapper;
//
//    @Override
//    public Object callMatch(Object matchRequest) throws Exception {
//        List<Object> matchRequests = new ArrayList<>();
//        matchRequests.add(matchRequest);
//        return callMatch(matchRequests).get(0);
//    }
//
//    @Override
//    public List<Object> callMatch(List<Object> matchRequests) throws Exception {
//        List<Object> results = new ArrayList<>();
//
//        FiniteDuration duration = new FiniteDuration(10, TimeUnit.MINUTES);
//        Timeout timeout = new Timeout(duration);
//        List<Future<Object>> matchFutures = new ArrayList<>();
//        for (Object matchRequest : matchRequests) {
//            GuideBook gb = wrapper.createGuideBook();
//            MatchTraveler traveler = new MatchTraveler(UUID.randomUUID().toString(), gb);
//            traveler.setData(matchRequest);
//
//            matchFutures.add(askMatchActor(traveler, timeout));
//        }
//
//        for (Future<Object> future : matchFutures) {
//            MatchTraveler result = (MatchTraveler) Await.result(future, timeout.duration());
//            log.info("Got result: " + result.getData());
//            results.add(result);
//        }
//        return results;
//    }
//
//    private Future<Object> askMatchActor(MatchTraveler traveler, Timeout timeout) {
//        return Patterns.ask(wrapper.getMatchActor(), traveler, timeout);
//    }
//
//}
