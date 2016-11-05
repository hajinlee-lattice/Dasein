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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;

import com.latticeengines.common.exposed.util.JsonUtils;

public abstract class Traveler {

    private static final Log log = LogFactory.getLog(Traveler.class);

    private final String rootOperationUid;
    private final String travelerId;
    private final List<TravelLog> travelStory = new ArrayList<>();
    private final Map<String, Set<String>> visitedHistory = new HashMap<>();
    private final Map<String, Long> checkpoints = new HashMap<>();
    private final Queue<String> visitingQueue = new LinkedList<>();
    private TravelException travelException;
    private Object result;
    private String originalLocation;
    private String anchorActorLocation;
    private StopWatch stopWatch;

    public Traveler(String rootOperationUid) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
    }

    protected abstract Object getInputData();

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getTravelerId() {
        return travelerId;
    }

    public List<TravelLog> getTravelStory() {
        return travelStory;
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
        travelStory.add(new TravelLog(Level.WARN, throwable, prefixByAge(message)));
    }

    public void warn(String message) {
        travelStory.add(new TravelLog(Level.WARN, prefixByAge(message)));
    }

    public void info(String message) {
        travelStory.add(new TravelLog(Level.INFO, prefixByAge(message)));
    }

    public void debug(String message) {
        travelStory.add(new TravelLog(Level.DEBUG, prefixByAge(message)));
    }

    private String prefixByAge(String message) {
        stopWatch.split();
        String newMessage = "[" + stopWatch.toSplitString() + "] " + message;
        stopWatch.unsplit();
        return newMessage;
    }

    public void checkIn(String site) {
        debug("Arrived " + site + ".");
        checkpoints.put(site, age());
    }

    public void checkOut(String site, String nextSite) {
        if (checkpoints.containsKey(site)) {
            Long duration = age() - checkpoints.get(site);
            debug(String.format("Spend Duration=%d ms at %s, and is now heading to %s", duration, site, nextSite));
            // TODO: generate a metric data point
        }
    }

    public void start() {
        stopWatch = new StopWatch();
        stopWatch.start();
        debug("Started the journey.");
    }

    public void finish() {
        String mood = getResult() != null ? "happily" : "sadly";
        debug(String.format("Ended the journey %s after Duration=%d ms.", mood, age()));
        stopWatch.stop();
    }

    private Long age() {
        stopWatch.split();
        Long age = stopWatch.getSplitTime();
        stopWatch.unsplit();
        return age;
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%s]", getClass().getSimpleName(), getTravelerId(), getRootOperationUid());
    }

}
