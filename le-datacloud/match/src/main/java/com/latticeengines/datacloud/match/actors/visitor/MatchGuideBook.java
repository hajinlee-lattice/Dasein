//package com.latticeengines.datacloud.match.actors.visitor;
//
//import com.latticeengines.actors.exposed.traveler.GuideBook;
//import com.latticeengines.actors.exposed.traveler.TravelException;
//import com.latticeengines.actors.exposed.traveler.Traveler;
//
//import akka.actor.ActorRef;
//
//public class MatchGuideBook implements GuideBook {
//    private final MatchActorStateTransitionGraph actorStateTransitionGraph;
//
//    public MatchGuideBook(MatchActorStateTransitionGraph actorStateTransitionGraph) {
//        this.actorStateTransitionGraph = actorStateTransitionGraph;
//    }
//
//    @Override
//    public Object getDestination(Object currentActorRef, Traveler traveler) {
//        Object destinationActorRef = traveler.getOriginalSender();
//        if (currentActorRef == null || currentActorRef instanceof ActorRef) {
//            if (traveler.getResult() == null) {
//                destinationActorRef = actorStateTransitionGraph//
//                        .next((ActorRef) currentActorRef, traveler);
//            }
//        } else {
//            throw new TravelException("Incorrect type got currentActorRef: " + currentActorRef);
//        }
//        return destinationActorRef;
//    }
//}
