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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public abstract class Traveler {

    private static final Log log = LogFactory.getLog(Traveler.class);

    private final String rootOperationUid;
    private final String travelerId;
    private final List<TravelLog> travelLogs;
    private final Map<String, Set<String>> visitedHistory;
    private TravelException travelException;
    private Object result;
    private String originalLocation;
    private String anchorActorLocation;
    private Queue<String> visitingQueue;

    public Traveler(String rootOperationUid) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
        this.travelLogs = new ArrayList<>();
        visitedHistory = new HashMap<>();
        visitingQueue = new LinkedList<>();
    }

    protected abstract Object getInputData();

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getTravelerId() {
        return travelerId;
    }

    public List<TravelLog> getTravelLogs() {
        return travelLogs;
    }

    public Map<String, Set<String>> getVisitedHistory() {
        return visitedHistory;
    }

    public void logVisitHistory(String traversedActor) {
        if (!visitedHistory.containsKey(traversedActor)) {
            visitedHistory.put(traversedActor, new HashSet<String>());
        }
        visitedHistory.get(traversedActor).add(JsonUtils.serialize(getInputData()));
    }

    public TravelException getTravelException() {
        return travelException;
    }

    public void setTravelException(TravelException travelException) {
        this.travelException = travelException;
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

    public void clearLocationsToVisitingQueue() {
        visitingQueue.clear();
    }

    public Queue<String> getVisitingQueue() {
        return visitingQueue;
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

    public void warn(String message, Throwable throwable) {
        travelLogs.add(new TravelLog(TravelLog.Level.WARN, throwable, message));
        log.warn(message, throwable);
    }

    public void warn(String message) {
        travelLogs.add(new TravelLog(TravelLog.Level.WARN, message));
        log.warn(message);
    }

    public void info(String message) {
        travelLogs.add(new TravelLog(TravelLog.Level.INFO, message));
        log.info(message);
    }

    public void debug(String message) {
        travelLogs.add(new TravelLog(TravelLog.Level.DEBUG, message));
        log.debug(message);
    }

    @Override
    public String toString() {
        return String.format("Traveler[%s:%s]", getTravelerId(), getRootOperationUid());
    }

}
