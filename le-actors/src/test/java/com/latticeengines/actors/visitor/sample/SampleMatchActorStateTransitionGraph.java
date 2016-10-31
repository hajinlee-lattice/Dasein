package com.latticeengines.actors.visitor.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.util.CollectionUtils;

import com.latticeengines.actors.exposed.traveler.TravelContext;

public class SampleMatchActorStateTransitionGraph {
    private List<String> dummyGraph = new ArrayList<>();
    private Map<String, String> dataSourceActors = new HashMap<>();

    @SuppressWarnings("unchecked")
    public SampleMatchActorStateTransitionGraph(String... actorRefs) {
        if (actorRefs != null) {
            dummyGraph.addAll(CollectionUtils.arrayToList(actorRefs));
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

    public void setDataSourceActors(Map<String, String> dataSourceActors) {
        this.dataSourceActors.putAll(dataSourceActors);
    }

    public Map<String, String> getDataSourceActors() {
        return dataSourceActors;
    }

}
