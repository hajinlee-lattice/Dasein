package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelerContext;

public class SampleMatchGuideBook implements GuideBook {
    private final SampleMatchActorStateTransitionGraph actorStateTransitionGraph;

    public SampleMatchGuideBook(SampleMatchActorStateTransitionGraph actorStateTransitionGraph) {
        this.actorStateTransitionGraph = actorStateTransitionGraph;
    }

    @Override
    public String next(String currentLocation, TravelerContext traveler) {
        String destinationLocation = null;
        if (traveler.getResult() == null && currentLocation != null) {
            destinationLocation = actorStateTransitionGraph//
                    .next(currentLocation, traveler, traveler.getOriginalLocation());
        } else if (currentLocation == null) {
            destinationLocation = actorStateTransitionGraph.getDummyGraph().get(0);
        }
        return destinationLocation;
    }

    @Override
    public String getDataSourceActorPath(String dataSourceName) {

        return actorStateTransitionGraph.getDataSourceActors().get(dataSourceName);
    }
}
