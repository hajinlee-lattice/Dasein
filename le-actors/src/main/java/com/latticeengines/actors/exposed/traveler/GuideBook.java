package com.latticeengines.actors.exposed.traveler;

public interface GuideBook {
    Object getDestination(Object currentActorRef, Traveler traveler);
}
