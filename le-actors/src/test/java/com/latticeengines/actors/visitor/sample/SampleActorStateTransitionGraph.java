package com.latticeengines.actors.visitor.sample;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.CollectionUtils;

import com.latticeengines.actors.exposed.traveler.Traveler;

import akka.actor.ActorRef;

public class SampleActorStateTransitionGraph {
    private List<ActorRef> dummyGraph = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public SampleActorStateTransitionGraph(ActorRef... actorRefs) {
        if (actorRefs != null) {
            dummyGraph.addAll(CollectionUtils.arrayToList(actorRefs));
        }
    }

    public ActorRef next(ActorRef currentActorRef, Traveler traveler) {
        int idx = 0;
        for (ActorRef node : dummyGraph) {
            if (currentActorRef == node) {
                if (++idx < dummyGraph.size()) {
                    return dummyGraph.get(idx);
                }
            }
            idx++;
        }

        return currentActorRef;
    }

}
