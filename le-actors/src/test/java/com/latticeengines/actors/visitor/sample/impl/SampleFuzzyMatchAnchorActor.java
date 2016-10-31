//package com.latticeengines.actors.visitor.sample.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.latticeengines.actors.exposed.traveler.Response;
//import com.latticeengines.actors.exposed.traveler.TravelContext;
//import com.latticeengines.actors.visitor.VisitorActorTemplate;
//import com.latticeengines.actors.visitor.sample.SampleMatchTravelerContext;
//
//import akka.actor.ActorRef;
//
//public class SampleFuzzyMatchAnchorActor extends VisitorActorTemplate {
//    private static final Log log = LogFactory.getLog(SampleFuzzyMatchAnchorActor.class);
//
//    @Override
//    protected Log getLogger() {
//        return log;
//    }
//
//    @Override
//    protected boolean isValidMessageType(Object msg) {
//        return msg instanceof SampleMatchTravelerContext || msg instanceof Response;
//    }
//
//    @Override
//    protected boolean process(TravelContext traveler) {
//        traveler.setAnchorActorLocation(self().path().toSerializationFormat());
//        List<String> nextLocations = calculateNextVisitingActors(traveler);
//        String[] nextLocationArray = new String[nextLocations.size()];
//        int idx = 0;
//        for (String location : nextLocations) {
//            nextLocationArray[idx++] = location;
//        }
//        traveler.setLocationInVisitingQueue(nextLocationArray);
//        return false;
//    }
//
//    private List<String> calculateNextVisitingActors(TravelContext traveler) {
//        String next = null;
//        if (traveler.getVisitedHistory().size() == 0) {
//            next = traveler.getGuideBook().next(null, traveler);
//        } else {
//            String latestMicroEngineLocation = traveler.getVisitedHistory()
//                    .get(traveler.getVisitedHistory().size() - 1);
//            next = traveler.getGuideBook().next(latestMicroEngineLocation, traveler);
//        }
//        List<String> visitingActors = new ArrayList<>();
//        visitingActors.add(next);
//        return visitingActors;
//    }
//
//    @Override
//    protected void process(Response response) {
//        // may be do something
//    }
//
//    @Override
//    protected void setOriginalSender(TravelContext traveler, ActorRef originalSender) {
//        if (traveler.getOriginalLocation() == null) {
//            traveler.setOriginalLocation(originalSender.path().toSerializationFormat());
//        }
//    }
//
//    @Override
//    protected void handleResult(Response response, TravelContext traveler) {
//        String originalLocation = traveler.getOriginalLocation();
//
//        ActorRef nextActorRef = getContext().actorFor(originalLocation);
//
//        getLogger().info("Send message to " + nextActorRef);
//
//        sendResult(nextActorRef, traveler.getResult());
//    }
//}
