package com.latticeengines.actors.exposed.traveler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import com.latticeengines.common.exposed.util.JsonUtils;

public abstract class TravelContext {

    private final String rootOperationUid;
    private final String travelerId;
    private final List<String> travelLogs;
    private final List<TravelWarning> travelWarnings;
    private final Map<String, Set<String>> visitedHistory;
    private TravelException travelException;
    private Map<String, Object> dataKeyValueMap;
    private Object result;
    private String originalLocation;
    private String anchorActorLocation;
    private Queue<String> visitingQueue;

    public TravelContext(String rootOperationUid) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
        this.travelLogs = new ArrayList<>();
        travelWarnings = new ArrayList<>();
        visitedHistory = new HashMap<>();
        visitingQueue = new LinkedList<>();
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getTravelerId() {
        return travelerId;
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

    public Map<String, Set<String>> getVisitedHistory() {
        return visitedHistory;
    }

    public void logVisitHistory(String traversedActor) {
        if (!visitedHistory.containsKey(traversedActor)) {
            visitedHistory.put(traversedActor, new HashSet<String>());
        }
        visitedHistory.get(traversedActor).add(JsonUtils.serialize(dataKeyValueMap));
    }

    public void clearVisitedHistory() {
        visitedHistory.clear();
    }

    public TravelException getTravelException() {
        return travelException;
    }

    public void setTravelException(TravelException travelException) {
        this.travelException = travelException;
    }

    public Map<String, Object> getDataKeyValueMap() {
        return dataKeyValueMap;
    }

    public void setDataKeyValueMap(Map<String, Object> dataKeyValueMap) {
        this.dataKeyValueMap = dataKeyValueMap;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getOriginalLocation() {
        return originalLocation;
    }

    public void setOriginalLocation(String originalLocation) {
        this.originalLocation = originalLocation;
    }

    public String getNextLocationFromVisitingQueue() {
        return visitingQueue.poll();
    }

    @SuppressWarnings("unchecked")
    public void addLocationsToVisitingQueue(String... nextLocations) {
        for (String location : nextLocations) {
            if (!visitingQueue.contains(location)) {
                visitingQueue.add(location);
            }
        }
    }

    public boolean visitingQueueIsEmpty() {
        return visitingQueue.isEmpty();
    }

    public String getAnchorActorLocation() {
        return anchorActorLocation;
    }

    public void setAnchorActorLocation(String anchorActorLocation) {
        this.anchorActorLocation = anchorActorLocation;
    }

}
