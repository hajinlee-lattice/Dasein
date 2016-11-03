package com.latticeengines.actors.visitor.sample;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.actors.exposed.traveler.Traveler;

public class SampleMatchActorStateTransitionGraph {
    private List<String> dummyGraph = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public SampleMatchActorStateTransitionGraph(String... actorRefs) {
        if (actorRefs != null) {
            for (String ref : actorRefs) {
                dummyGraph.add(ref);
            }
        }
    }

    public List<String> getDummyGraph() {
        return dummyGraph;
    }

    public String next(String currentLocation, Traveler traveler, String originalLocation) {
        int idx = 0;
        for (String node : dummyGraph) {
            if (node.equals(currentLocation)) {
                if (++idx < dummyGraph.size()) {
                    return dummyGraph.get(idx);
                }
            } else if (currentLocation == null) {
                return dummyGraph.get(idx);
            }
            idx++;
        }

        return originalLocation;
    }
}
