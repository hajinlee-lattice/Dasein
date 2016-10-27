package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.VisitorActorTemplate;

import akka.actor.ActorRef;

public abstract class MatchActorTemplate extends VisitorActorTemplate {

    @Override
    protected boolean isValidTravelerType(Traveler traveler) {
        return traveler instanceof MatchTraveler;
    }

    @Override
    public void travel(Traveler traveler, Object nextActorRef, Object currentActorRef) {
        traveler.updateTraversedActorInfo(currentActorRef);
        ((ActorRef) nextActorRef).tell(traveler, (ActorRef) currentActorRef);
    }
}
