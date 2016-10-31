//package com.latticeengines.actors.visitor.sample;
//
//import com.latticeengines.actors.exposed.traveler.GuideBook;
//import com.latticeengines.actors.exposed.traveler.Response;
//import com.latticeengines.actors.exposed.traveler.TravelContext;
//import com.latticeengines.actors.visitor.VisitorActorTemplate;
//
//import akka.actor.ActorRef;
//
//public abstract class SampleMicroEngineActorTemplate extends VisitorActorTemplate {
//    protected abstract String getDataSourceActor();
//
//    protected abstract boolean accept(TravelContext traveler);
//
//    @Override
//    protected boolean isValidMessageType(Object msg) {
//        return msg instanceof SampleMatchTravelerContext || msg instanceof Response;
//    }
//
//    @Override
//    protected boolean process(TravelContext traveler) {
//        if (accept(traveler)) {
//            String dataSourceActor = getDataSourceActor();
//            dataSourceActor = traveler.getGuideBook().getDataSourceActorPath(dataSourceActor);
//            ActorRef nextActorRef = getContext().actorFor(dataSourceActor);
//
//            SampleDataSourceLookupRequest req = new SampleDataSourceLookupRequest();
//            req.setMatchTravelerContext((SampleMatchTravelerContext) traveler);
//            req.setInputData(traveler.getDataKeyValueMap());
//            traveler.logVisit(self().path().toSerializationFormat());
//
//            nextActorRef.tell(req, self());
//            return true;
//        } else {
//            traveler.logVisit(self().path().toSerializationFormat());
//            return false;
//        }
//    }
//
//    @Override
//    protected void process(Response response) {
//        // may be do something
//    }
//
//    @Override
//    protected String getNextLocation(TravelContext traveler) {
//        GuideBook guideBook = traveler.getGuideBook();
//        String nextLocation = guideBook.next(getSelf().path().toSerializationFormat(), traveler);
//        return nextLocation;
//    }
//}
