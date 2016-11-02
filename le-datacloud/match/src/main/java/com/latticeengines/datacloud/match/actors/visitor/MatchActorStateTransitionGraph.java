package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.actors.exposed.traveler.TravelContext;

public class MatchActorStateTransitionGraph {
    private List<String> dummyGraph = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public MatchActorStateTransitionGraph(String... actorRefs) {
        if (actorRefs != null) {
            for (String ref : actorRefs) {
                dummyGraph.add(ref);
            }
        }
    }

    public List<String> getDummyGraph() {
        return dummyGraph;
    }

    public String next(String currentLocation, TravelContext traveler, String originalLocation) {
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
