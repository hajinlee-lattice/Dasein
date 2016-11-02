package com.latticeengines.actors.exposed.traveler;

public abstract class GuideBook {
    public abstract String next(String currentLocation, Traveler context);

    public void logVisit(String traversedActor, Traveler context) {
        context.logVisitHistory(traversedActor);
    }
}
