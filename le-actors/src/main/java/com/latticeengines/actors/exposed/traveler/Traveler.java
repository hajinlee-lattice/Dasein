package com.latticeengines.actors.exposed.traveler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class Traveler {

    private final String rootOperationUid;
    private final String travelerId;
    private final GuideBook guideBook;
    private final List<String> travelLogs;
    private final List<TravelWarning> travelWarnings;
    private final List<Object> traversedActors;
    private final Map<Object, Integer> actorTraversalCountMap;
    private TravelException travelException;
    private Object data;
    private Object result;
    private Object originalSender;

    public Traveler(String rootOperationUid, GuideBook guideBook) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
        this.guideBook = guideBook;
        this.travelLogs = new ArrayList<>();
        travelWarnings = new ArrayList<>();
        traversedActors = new ArrayList<>();
        actorTraversalCountMap = new HashMap<>();
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getTravelerId() {
        return travelerId;
    }

    public GuideBook getGuideBook() {
        return guideBook;
    }

    public List<String> getTravelLogs() {
        return travelLogs;
    }

    public void setTravelLog(String travelLog) {
        travelLogs.add(travelLog);
    }
    
    public List<TravelWarning> getTravelWarnings() {
        return travelWarnings;
    }

    public void setTravelWarning(TravelWarning travelWarning) {
        travelWarnings.add(travelWarning);
    }

    public List<Object> getTraversedActors() {
        return traversedActors;
    }

    public Map<Object, Integer> getActorTraversalCountMap() {
        return actorTraversalCountMap;
    }

    public void updateTraversedActorInfo(Object traversedActor) {
        traversedActors.add(traversedActor);
        if (!actorTraversalCountMap.containsKey(traversedActor)) {
            actorTraversalCountMap.put(traversedActor, 0);
        }
        int actorTraversalCount = actorTraversalCountMap.get(traversedActor) + 1;
        actorTraversalCountMap.put(traversedActor, actorTraversalCount);
    }

    public TravelException getTravelException() {
        return travelException;
    }

    public void setTravelException(TravelException travelException) {
        this.travelException = travelException;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getOriginalSender() {
        return originalSender;
    }

    public void setOriginalSender(Object originalSender) {
        this.originalSender = originalSender;
    }

}
