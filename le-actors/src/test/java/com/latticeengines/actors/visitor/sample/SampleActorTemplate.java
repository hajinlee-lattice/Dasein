package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.VisitorActorTemplate;

import akka.actor.ActorRef;

public abstract class SampleActorTemplate extends VisitorActorTemplate {

    @Override
    protected boolean isValidTravelerType(Traveler traveler) {
        return traveler instanceof SampleTraveler;
    }

    @Override
    public void travel(Traveler traveler, Object nextActorRef, Object currentActorRef) {
        traveler.updateTraversedActorInfo(currentActorRef);
        ((ActorRef) nextActorRef).tell(traveler, (ActorRef) currentActorRef);
    }
}
