package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelException;
import com.latticeengines.actors.exposed.traveler.Traveler;

import akka.actor.ActorRef;

public class SampleGuideBook implements GuideBook {
    private final SampleActorStateTransitionGraph actorStateTransitionGraph;

    public SampleGuideBook(SampleActorStateTransitionGraph actorStateTransitionGraph) {
        this.actorStateTransitionGraph = actorStateTransitionGraph;
    }

    @Override
    public Object getDestination(Object currentActorRef, Traveler traveler) {
        Object destinationActorRef = traveler.getOriginalSender();
        if (currentActorRef == null || currentActorRef instanceof ActorRef) {
            if (traveler.getResult() == null) {
                destinationActorRef = actorStateTransitionGraph//
                        .next((ActorRef) currentActorRef, traveler);
            }
        } else {
            throw new TravelException("Incorrect type got currentActorRef: " + currentActorRef);
        }
        return destinationActorRef;
    }
}
