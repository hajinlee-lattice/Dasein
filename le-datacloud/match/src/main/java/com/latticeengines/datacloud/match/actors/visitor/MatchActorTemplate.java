//package com.latticeengines.datacloud.match.actors.visitor;
//
//import com.latticeengines.actors.exposed.traveler.GuideBook;
//import com.latticeengines.actors.exposed.traveler.TravelerContext;
//import com.latticeengines.actors.visitor.VisitorActorTemplate;
//
//import akka.actor.ActorRef;
//
//public abstract class MatchActorTemplate extends VisitorActorTemplate {
//
//    @Override
//    protected boolean isValidMessageType(TravelerContext traveler) {
//        return traveler instanceof MatchTravelerContext;
//    }
//
//    @Override
//    public void travel(TravelerContext traveler, Object nextActorRef, Object currentActorRef) {
//        traveler.updateVisitedHistoryInfo(((ActorRef) currentActorRef).path().toSerializationFormat());
//        ((ActorRef) nextActorRef).tell(traveler, (ActorRef) currentActorRef);
//    }
//
//    @Override
//    protected String getNextLocation(TravelerContext traveler) {
//        GuideBook guideBook = traveler.getGuideBook();
//        String nextLocation = guideBook.next(//
//                getSelf().path().toSerializationFormat(), traveler);
//        return nextLocation;
//    }
//}
