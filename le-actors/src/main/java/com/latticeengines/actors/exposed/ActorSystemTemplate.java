package com.latticeengines.actors.exposed;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.domain.exposed.actors.ActorType;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Future;

public abstract class ActorSystemTemplate {
    private static final Logger log = LoggerFactory.getLogger(ActorSystemTemplate.class);

    /*********************
     * Abstract methods
     *********************/

    // Start anchor actors, initialize anchorPaths map
    protected abstract void initAnchors();

    // Start junction actors, initialize actorNameToType &
    // actorNameAbbrToType maps
    protected abstract void initJunctions();

    // Start micro-engine actors, initialize actorNameToType &
    // actorNameAbbrToType maps
    protected abstract void initMicroEngines();

    // Start metrics actors
    protected abstract void initMetricActors();

    // Preparation work besides actor bootstraps to initialize an actor system
    protected abstract void postInitialize();

    public abstract ActorRef getAnchor();


    /*********************
     * Members
     *********************/

    protected ActorSystem system;
    protected ActorFactory actorFactory;

    // ActorName -> ActorRef
    protected final ConcurrentMap<String, ActorRef> actorRefMap = new ConcurrentHashMap<>();
    // ActorPath -> ActorName
    protected final ConcurrentMap<String, String> actorPathMap = new ConcurrentHashMap<>();
    // ActorName -> ActorType
    protected final ConcurrentMap<String, ActorType> actorNameToType = new ConcurrentHashMap<>();
    // ActorNameAbbr -> ActorType
    // ActorNameAbbr is actor name without actor type as suffix which is used to
    // configure decision graph
    protected final ConcurrentMap<String, ActorType> actorNameAbbrToType = new ConcurrentHashMap<>();


    /***********
     * Actors
     ***********/

    /**
     * For actors named by class name
     * 
     * @param actorClz
     * @return
     */
    public <T extends ActorTemplate> ActorRef getActorRef(Class<T> actorClz) {
        return getActorRef(actorClz.getSimpleName());
    }

    /**
     * For actors sharing same class but with different actor names
     * 
     * @param actorName
     * @return
     */
    public ActorRef getActorRef(String actorName) {
        return getActorRefMap().get(actorName);
    }

    /**
     * Every actor has unique path
     * 
     * @param actorPath
     * @return
     */
    public String getActorName(String actorPath) {
        return getActorPathMap().get(actorPath);
    }

    /**
     * Every actor has unique reference
     * 
     * @param actorRef
     * @return
     */
    public String getActorName(ActorRef actorRef) {
        String path = ActorUtils.getPath(actorRef);
        return getActorName(path);
    }

    /**
     * Get actor type by actor name
     * 
     * @param actorName
     * @return
     */
    public ActorType getActorType(String actorName) {
        return getActorNameToType().get(actorName);
    }

    /**
     * Get actor type by actor name abbreviation
     * 
     * @param actorNameAbbr
     * @return
     */
    public ActorType getActorTypeByNameAbbr(String actorNameAbbr) {
        return getActorNameAbbrToType().get(actorNameAbbr);
    }

    /**
     * Metrics actor is to publish performance metrics to external metrics
     * system, eg. InfluxDB
     * 
     * @return
     */
    public ActorRef getMetricActor() {
        initialize();
        return getActorRef(MetricActor.class);
    }

    /************
     * Message
     ************/

    public Future<Object> askAnchor(Traveler traveler, Timeout timeout) {
        initialize();
        return Patterns.ask(getAnchor(), traveler, timeout);
    }

    public void sendResponse(Object response, String returnAddress) {
        getActorSystem().actorSelection(returnAddress).tell(response, null);
    }

    /*******************
     * Initialization
     *******************/
    protected void initialize() {
        if (system == null) {
            synchronized (this) {
                if (system == null) {
                    system = ActorSystemFactory.create(getSystemName(), getDispatcherNum());
                    initActors();
                    postInitialize();
                }
            }
        }
    }

    protected String getSystemName() {
        return this.getClass().getSimpleName();
    }

    protected int getDispatcherNum() {
        return 16;
    }

    protected void initActors() {
        initAnchors();
        initJunctions();
        initMicroEngines();
        initMetricActors();
        initAssistantActors();

        log.info("All actors created.");
    }

    protected void initAssistantActors() {

    }

    protected <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz) {
        return initNamedActor(actorClz, false, 1);
    }

    protected <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting,
            int routingCardinality) {
        return initNamedActor(actorClz, useRouting, routingCardinality, actorClz.getSimpleName());
    }

    protected <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting,
            int routingCardinality, String actorName) {
        ActorRef actorRef;
        if (useRouting) {
            actorRef = actorFactory.create(system, actorName, actorClz, RoutingLogic.RoundRobinRoutingLogic,
                    routingCardinality);
        } else {
            actorRef = actorFactory.create(system, actorName, actorClz);
        }
        actorRefMap.put(actorName, actorRef);
        actorPathMap.putIfAbsent(ActorUtils.getPath(actorRef), actorName);
        log.info(String.format("Add actor-ref %s with class %s", actorName, actorClz.getSimpleName()));
        return actorRef;
    }

    private ActorSystem getActorSystem() {
        initialize();
        return system;
    }

    private ConcurrentMap<String, ActorRef> getActorRefMap() {
        initialize();
        return actorRefMap;
    }

    private ConcurrentMap<String, String> getActorPathMap() {
        initialize();
        return actorPathMap;
    }

    private ConcurrentMap<String, ActorType> getActorNameToType() {
        initialize();
        return actorNameToType;
    }

    private ConcurrentMap<String, ActorType> getActorNameAbbrToType() {
        initialize();
        return actorNameAbbrToType;
    }
}
