package com.latticeengines.actors.exposed.traveler;

public abstract class GuideBook {
    public abstract String next(String currentLocation, TravelContext context);

    public void logVisit(String traversedActor, TravelContext context) {
        context.logVisit(traversedActor);
    }
}
