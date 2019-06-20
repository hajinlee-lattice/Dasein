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

    // Travel errors
    private List<String> travelErrors;

    // ONLY for testing purpose, set to true: Traveler will ignore decision
    // graph and return to sender after processing
    private boolean returnSender;

    // ONLY for testing purpose, force to inject exception at actor with
    // designated name, or system at designated class name
    private String actorOrServiceToInjectFailure;

    /***********************************
     * Bound to current decision graph
     ***********************************/
    // Goal of travel in current decision graph
    private Object result;

    private String decisionGraph;

    // Actor path -> Input data of every visit
    private Map<String, Set<String>> visitedHistory = new HashMap<>();

    // To capture how much time the travel spends in current actor
    // For junction actor, travel time in transfered decision graph is counted
    // For micro-engine actor which needs external assistant actor, time in
    // external assistant actor is also counted
    // actor name -> duration
    private Map<String, Long> checkpoints = new HashMap<>();

    // Nodes in decision graph to be visited sequentially
    private Queue<String> visitingQueue = new LinkedList<>();

    // Catch exception during traveling
    private TravelException travelException;

    // When the 1st time anchor receives the traveler in current decision graph,
    // isProcessed will be set as true
    private boolean isProcessed = false;

    // Set with anchor location of current decision graph
    private String anchorActorLocation;

    // Number of retries in current decision graph (1st try is counted as
    // retries = 1, not 0)
    private int retries = 0;


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

    public void clearResult() {
        this.result = null;
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

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void addRetry() {
        this.retries++;
    }

    public void descRetry() {
        this.retries--;
    }

    public boolean isReturnSender() {
        return returnSender;
    }

    public void setReturnSender(boolean returnSender) {
        this.returnSender = returnSender;
    }

    public String getActorOrServiceToInjectFailure() {
        return actorOrServiceToInjectFailure;
    }

    public void setActorOrServiceToInjectFailure(String actorOrServiceToInjectFailure) {
        this.actorOrServiceToInjectFailure = actorOrServiceToInjectFailure;
    }

    public List<String> getTravelErrors() {
        return travelErrors;
    }

    public void setTravelErrors(List<String> travelErrors) {
        this.travelErrors = travelErrors;
    }

    /********************
     * Business methods
     ********************/

    public void logVisitHistory(String traversedActor) {
        if (!visitedHistory.containsKey(traversedActor)) {
            visitedHistory.put(traversedActor, new HashSet<String>());
        }
        visitedHistory.get(traversedActor)
                .add(getInputData() == null ? "" : getInputData().toString());
    }

    // Don't call it as getXXX. When serializing the object, getXXX is called
    // and visitingQueue is polled which causes unexpected behavior
    public String findNextLocationFromVisitingQueue() {
        return visitingQueue.poll();
    }

    public void addLocationsToVisitingQueue(String... nextLocations) {
        for (String location : nextLocations) {
            if (!visitingQueue.contains(location)) {
                visitingQueue.add(location);
            }
        }
    }

    // When unexpected issue happens, nodes to be visited are cleared. Force to
    // return anchor
    public void clearLocationsToVisitingQueue() {
        visitingQueue.clear();
    }

    public Queue<String> getVisitingQueue() {
        return visitingQueue;
    }

    public boolean visitingQueueIsEmpty() {
        return visitingQueue.isEmpty();
    }

    public void error(String message, Throwable throwable) {
        if (Level.ERROR.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.ERROR, throwable, prefixByAge(message)));
            logTravelErrors(message);
            setTravelException(new TravelException(message, throwable));
        }
    }

    public void error(String message) {
        if (Level.ERROR.isGreaterOrEqual(logLevel)) {
            travelStory.add(new TravelLog(Level.ERROR, prefixByAge(message)));
            logTravelErrors(message);
        }
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

    /**
     * If need to re-travel at anchor, clean up necessary status
     */
    public void prepareForRetravel() {
        isProcessed = false;
        result = null;
        visitedHistory.clear();
        checkpoints.clear();
        visitingQueue.clear();
        if (travelErrors != null) {
            travelErrors.clear();
        }
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%s]", getClass().getSimpleName(), getTravelerId(), getRootOperationUid());
    }

    public void pushToTransitionHistory(String junction, boolean clearCurrent) {
        TransitionHistory snapshot = new TransitionHistoryBuilder()//
                .withJunction(junction) //
                .withDecisionGraph(decisionGraph) //
                .withVisitedHistory(visitedHistory) //
                .withCheckpoints(checkpoints) //
                .withVisitingQueue(visitingQueue) //
                .withRetries(retries) //
                .withOthers(getOtherTransitionHistoryToPush()) //
                .build();
        transitionHistory.push(snapshot);
        if (clearCurrent) {
            visitedHistory = new HashMap<>();
            checkpoints = new HashMap<>();
            visitingQueue = new LinkedList<>();
            retries = 0;
            decisionGraph = null;
            clearOtherCurrentTransitionHistory();
        }
    }

    public void recoverTransitionHistory() {
        TransitionHistory snapshot = transitionHistory.pop();
        visitedHistory = snapshot.getVisitedHistory();
        checkpoints = snapshot.getCheckpoints();
        visitingQueue = snapshot.getVisitingQueue();
        retries = snapshot.getRetries();
        decisionGraph = snapshot.getDecisionGraph();
        recoverOtherTransitionHistory(snapshot.getOthers());
    }

    protected List<Object> getOtherTransitionHistoryToPush() {
        return null;
    }

    protected void clearOtherCurrentTransitionHistory() {

    }

    protected void recoverOtherTransitionHistory(List<Object> others) {

    }

    public void logTravelErrors(List<String> travelErrors) {
        if (this.travelErrors == null) {
            this.travelErrors = new ArrayList<>();
        }
        this.travelErrors.addAll(travelErrors);
    }

    public void logTravelErrors(String travelError) {
        if (this.travelErrors == null) {
            this.travelErrors = new ArrayList<>();
        }
        this.travelErrors.add(travelError);
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
        private int retries;
        private List<Object> others;

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

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public List<Object> getOthers() {
            return others;
        }

        public void setOthers(List<Object> others) {
            this.others = others;
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

        public TransitionHistoryBuilder withRetries(int retries) {
            transitionHistory.setRetries(retries);
            return this;
        }

        public TransitionHistoryBuilder withOthers(List<Object> others) {
            transitionHistory.setOthers(others);
            return this;
        }

        public TransitionHistory build() {
            return transitionHistory;
        }
    }

}
