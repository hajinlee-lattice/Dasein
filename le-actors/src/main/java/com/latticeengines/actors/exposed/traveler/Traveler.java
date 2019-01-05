package com.latticeengines.actors.exposed.traveler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Level;

import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.actors.VisitingHistory;

import akka.util.Timeout;

public abstract class Traveler {

    protected abstract Object getInputData();

    /*************************
     * Bound to whole travel
     *************************/
    private final String rootOperationUid;
    private final String travelerId;
    private final List<TravelLog> travelStory = new ArrayList<>();

    // Set with 1st anchor location when the traveler comes into actor system
    // Will not change after jumping to sub decision graph
    // Purpose: Send traveler to this location to terminate traveling
    private String originalLocation;

    // Take snapshot of current traveler status at junction actor and push to
    // stack
    // After return back to junction actor, recover the traveler status from
    // stack
    private final Stack<TransitionHistory> transitionHistory = new Stack<>();

    private StopWatch stopWatch;
    private Level logLevel = Level.DEBUG;
    private String lastStop; // Only for metrics purpose
    private Double totalTravelTime; // only for metric purpose

    // If exceed time limit, treat as timeout failure
    private Timeout travelTimeout;

    /***********************************
     * Bound to current decision graph
     ***********************************/
    // Goal of travel
    private Object result;
    private String decisionGraph;
    // TODO(ZDD): Add comment
    private Map<String, Set<String>> visitedHistory = new HashMap<>();
    // TODO(ZDD): Add comment
    private Map<String, Long> checkpoints = new HashMap<>();
    // TODO(ZDD): Add comment
    private Queue<String> visitingQueue = new LinkedList<>();
    // TODO(ZDD): Add comment
    private TravelException travelException;
    // When the 1st time anchor receives the traveler in current decision graph,
    // isProcessed will be set as true
    private boolean isProcessed = false;
    // Set with anchor location of current decision graph
    private String anchorActorLocation;


    /*******************************
     * Constructor & Getter/Setter
     *******************************/
    public Traveler(String rootOperationUid) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
    }

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

    public void setLogLevel(Level logLevel) {
        this.logLevel = logLevel;
    }

    public String getAnchorActorLocation() {
        return anchorActorLocation;
    }

    public void setAnchorActorLocation(String anchorActorLocation) {
        this.anchorActorLocation = anchorActorLocation;
    }

    public Stack<TransitionHistory> getTransitionHistory() {
        return transitionHistory;
    }

    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(boolean isProcessed) {
        this.isProcessed = isProcessed;
    }

    @MetricTag(tag = "LastStop")
    public String getLastStop() {
        return lastStop;
    }

    public void setLastStop(String lastStop) {
        this.lastStop = lastStop;
    }

    @MetricTag(tag = "DecisionGraph")
    public String getDecisionGraph() {
        return decisionGraph;
    }

    public void setDecisionGraph(String decisionGraph) {
        this.decisionGraph = decisionGraph;
    }

    @MetricField(name = "TravelTime", fieldType = MetricField.FieldType.DOUBLE)
    public Double getTotalTravelTime() {
        return totalTravelTime;
    }

    public void recordTotalTime() {
        this.totalTravelTime = age().doubleValue();
    }

    public Timeout getTravelTimeout() {
        return travelTimeout;
    }

    public void setTravelTimeout(Timeout travelTimeout) {
        this.travelTimeout = travelTimeout;
    }

    /********************
     * Business methods
     ********************/

    public void logVisitHistory(String traversedActor) {
        if (!visitedHistory.containsKey(traversedActor)) {
            visitedHistory.put(traversedActor, new HashSet<String>());
        }
        // TODO(ZDD): It is to avoid repeated traversing actor with same input
        // data.
        // After supporting repeated runs in decision graph, this logic need to
        // revisit.
        // For entity match, match traveler starts with null matchKeyTuple, need
        // to revisit null handling
        visitedHistory.get(traversedActor)
                .add(getInputData() == null ? "" : getInputData().toString());
    }

    public String getNextLocationFromVisitingQueue() {
        return visitingQueue.poll();
    }

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

    public void warn(String message, Throwable throwable) {
        if (Level.WARN.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.WARN, throwable, prefixByAge(message)));
        }
    }

    public void warn(String message) {
        if (Level.WARN.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.WARN, prefixByAge(message)));
        }
    }

    public void info(String message) {
        if (Level.INFO.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.INFO, prefixByAge(message)));
        }
    }

    public void debug(String message) {
        if (Level.DEBUG.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.DEBUG, prefixByAge(message)));
        }
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

    public VisitingHistory checkOut(String site, String nextSite) {
        if (checkpoints.containsKey(site)) {
            Long duration = age() - checkpoints.get(site);
            debug(String.format("Spend Duration=%d ms at %s, and is now heading to %s", duration, site, nextSite));
            return generateVisitingMetric(site, duration);
        }
        return null;
    }

    public void start() {
        stopWatch = new StopWatch();
        stopWatch.start();
        debug("Started the journey. TravelerId=" + getTravelerId());
    }

    public void finish() {
        String mood = getResult() != null ? "happily" : "sadly";
        debug(String.format("Ended the journey %s after Duration=%d ms.", mood, age()));
        stopWatch.stop();
    }

    protected Long age() {
        stopWatch.split();
        Long age = stopWatch.getSplitTime();
        stopWatch.unsplit();
        return age;
    }

    protected VisitingHistory generateVisitingMetric(String site, Long duration) {
        return new VisitingHistory( this.travelerId, site, duration);
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%s]", getClass().getSimpleName(), getTravelerId(), getRootOperationUid());
    }

    // TODO: After moving decisionGraph from MatchTraveler to Traveler
    // 1. remove decisionGraph from parameter
    // 2. clear decisionGraph if clearCurrent = true
    public void pushToTransitionHistory(String junction, String decisionGraph, boolean clearCurrent) {
        TransitionHistory snapshot = new TransitionHistoryBuilder()//
                .withJunction(junction) //
                .withDecisionGraph(decisionGraph) //
                .withVisitedHistory(visitedHistory) //
                .withCheckpoints(checkpoints) //
                .withVisitingQueue(visitingQueue) //
                .build();
        transitionHistory.push(snapshot);
        if (clearCurrent) {
            visitedHistory.clear();
            checkpoints.clear();
            visitingQueue.clear();
        }
    }

    // TODO: After moving decisionGraph from MatchTraveler to Traveler
    // 1. Recover decision graph and no longer return
    public String recoverTransitionHistory() {
        TransitionHistory snapshot = transitionHistory.pop();
        visitedHistory = snapshot.getVisitedHistory();
        checkpoints = snapshot.getCheckpoints();
        visitingQueue = snapshot.getVisitingQueue();
        return snapshot.getDecisionGraph();
    }


    /**
     * To take snapshot of traveler status at junction actor
     */
    public static class TransitionHistory {
        private String junction;
        private String decisionGraph;
        private Map<String, Set<String>> visitedHistory;
        private Map<String, Long> checkpoints;
        private Queue<String> visitingQueue;

        public String getJunction() {
            return junction;
        }

        public void setJunction(String junction) {
            this.junction = junction;
        }

        public String getDecisionGraph() {
            return decisionGraph;
        }

        public void setDecisionGraph(String decisionGraph) {
            this.decisionGraph = decisionGraph;
        }

        public Map<String, Set<String>> getVisitedHistory() {
            return visitedHistory;
        }

        public void setVisitedHistory(Map<String, Set<String>> visitedHistory) {
            this.visitedHistory = visitedHistory;
        }

        public Map<String, Long> getCheckpoints() {
            return checkpoints;
        }

        public void setCheckpoints(Map<String, Long> checkpoints) {
            this.checkpoints = checkpoints;
        }

        public Queue<String> getVisitingQueue() {
            return visitingQueue;
        }

        public void setVisitingQueue(Queue<String> visitingQueue) {
            this.visitingQueue = visitingQueue;
        }

    }

    public static final class TransitionHistoryBuilder {
        private TransitionHistory transitionHistory;

        public TransitionHistoryBuilder() {
            transitionHistory = new TransitionHistory();
        }

        public TransitionHistoryBuilder withJunction(String junction) {
            transitionHistory.setJunction(junction);
            return this;
        }

        public TransitionHistoryBuilder withDecisionGraph(String decisionGraph) {
            transitionHistory.setDecisionGraph(decisionGraph);
            return this;
        }

        public TransitionHistoryBuilder withVisitedHistory(Map<String, Set<String>> visitedHistory) {
            Map<String, Set<String>> visitedHistoryCopy = visitedHistory.entrySet().stream()
                    .collect(Collectors.toMap(ent -> ent.getKey(), ent -> new HashSet<>(ent.getValue())));
            transitionHistory.setVisitedHistory(visitedHistoryCopy);
            return this;
        }

        public TransitionHistoryBuilder withCheckpoints(Map<String, Long> checkpoints) {
            transitionHistory.setCheckpoints(new HashMap<>(checkpoints));
            return this;
        }

        public TransitionHistoryBuilder withVisitingQueue(Queue<String> visitingQueue) {
            transitionHistory.setVisitingQueue(new LinkedList<>(visitingQueue));
            return this;
        }

        public TransitionHistory build() {
            return transitionHistory;
        }
    }

}
