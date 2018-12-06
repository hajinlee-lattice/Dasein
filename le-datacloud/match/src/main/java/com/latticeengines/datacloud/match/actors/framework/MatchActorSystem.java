package com.latticeengines.datacloud.match.actors.framework;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorFactory;
import com.latticeengines.actors.exposed.ActorSystemFactory;
import com.latticeengines.actors.exposed.MetricActor;
import com.latticeengines.actors.exposed.RoutingLogic;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.AccountMatchJunctionActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CDLAssociateActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CDLLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnBCacheLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryStateBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryZipCodeBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideBookLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchJunctionActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToCachedDunsMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchActorType;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Future;

@Component("matchActorSystem")
public class MatchActorSystem {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystem.class);

    private static final String BATCH_MODE = "batch";
    private static final String REALTIME_MODE = "realtime";

    private static final int MAX_ALLOWED_RECORD_COUNT_SYNC = 200;
    private static final int MAX_ALLOWED_RECORD_COUNT_ASYNC = 10000;
    private final AtomicInteger maxAllowedRecordCount = new AtomicInteger(MAX_ALLOWED_RECORD_COUNT_SYNC);

    private ActorSystem system;

    private boolean batchMode = false;

    @Value("${datacloud.match.dnbLookupActor.actor.cardinality:3}")
    private int dnbLookupActorCardinality;

    @Value("${datacloud.match.dnbCacheLookupActor.actor.cardinality:3}")
    private int dnbCacheLookupActorCardinality;

    @Value("${datacloud.match.dynamoLookupActor.actor.cardinality:10}")
    private int dynamoLookupActorCardinality;

    @Value("${datacloud.match.dunsGuideBookLookupActor.actor.cardinality:3}")
    private int dunsGuideBookLookupActorCardinality;

    @Value("${datacloud.match.cdlLookupActor.actor.cardinality:3}")
    private int cdlLookupActorCardinality;

    @Value("${datacloud.match.cdlAssociateActor.actor.cardinality:2}")
    private int cdlAssociateActorCardinality;

    @Value("${datacloud.match.metricActor.actor.cardinality:4}")
    private int metricActorCardinality;

    @Value("${datacloud.match.actor.datasource.default.threadpool.count.min}")
    private Integer defaultThreadpoolCountMin;

    @Value("${datacloud.match.actor.datasource.default.threadpool.count.max}")
    private Integer defaultThreadpoolCountMax;

    @Value("${datacloud.match.actor.datasource.default.threadpool.queue.size}")
    private Integer defaultThreadpoolQueueSize;

    private final ActorFactory actorFactory;

    private ExecutorService dataSourceServiceExecutor;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    // ActorName -> ActorRef
    private final ConcurrentMap<String, ActorRef> actorRefMap = new ConcurrentHashMap<>();
    // ActorPath -> ActorName
    private final ConcurrentMap<String, String> actorPathMap = new ConcurrentHashMap<>();
    // AnchorName -> AnchorPath
    // Multiple anchors share same anchor class but with different anchor name
    private final ConcurrentMap<String, String> anchorPaths = new ConcurrentHashMap<>();
    // ActorName -> ActorType
    private final ConcurrentMap<String, MatchActorType> actorNameToType = new ConcurrentHashMap<>();
    // ActorNameAbbr -> ActorType
    private final ConcurrentMap<String, MatchActorType> actorNameAbbrToType = new ConcurrentHashMap<>();


    @Autowired
    public MatchActorSystem(ActorFactory actorFactory) {
        this.actorFactory = actorFactory;
    }

    @PreDestroy
    private void preDestroy() {
        log.info("Shutting down match actor system");
        if (system != null) {
            system.shutdown();
        }
        if (dataSourceServiceExecutor != null) {
            dataSourceServiceExecutor.shutdown();
        }
        log.info("Completed shutdown of match actor system");
    }

    public <T extends ActorTemplate> ActorRef getActorRef(Class<T> actorClz) {
        return getActorRef(actorClz.getSimpleName());
    }

    public ActorRef getActorRef(String actorName) {
        return getActorRefMap().get(actorName);
    }

    String getActorName(String actorPath) {
        return getActorPathMap().get(actorPath);
    }

    public String getActorName(ActorRef actorRef) {
        String path = ActorUtils.getPath(actorRef);
        String clzName = getActorName(path);
        if (StringUtils.isEmpty(clzName)) {
            return path;
        } else {
            return clzName;
        }
    }

    ActorRef getMatchAnchor(MatchTraveler traveler) throws ExecutionException {
        DecisionGraph dg = matchDecisionGraphService.getDecisionGraph(traveler);
        return getActorRef(dg.getAnchor());
    }

    public ActorRef getMetricActor() {
        initialize();
        return getActorRef(MetricActor.class);
    }

    public Future<Object> askAnchor(MatchTraveler traveler, Timeout timeout) throws ExecutionException {
        initialize();
        return Patterns.ask(getMatchAnchor(traveler), traveler, timeout);
    }

    public void sendResponse(Object response, String returnAddress) {
        getActorSystem().actorSelection(returnAddress).tell(response, null);
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
        if (batchMode) {
            maxAllowedRecordCount.set(MAX_ALLOWED_RECORD_COUNT_ASYNC);
        } else {
            maxAllowedRecordCount.set(MAX_ALLOWED_RECORD_COUNT_SYNC);
        }
        log.info("Switch MatchActorSystem to " + (isBatchMode() ? BATCH_MODE : REALTIME_MODE) + " mode.");
    }

    public int getMaxAllowedRecordCount() {
        return maxAllowedRecordCount.get();
    }

    public ExecutorService getDataSourceServiceExecutor() {
        return dataSourceServiceExecutor;
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

    public String getAnchorPath(String anchorName) {
        initialize();
        return anchorPaths.get(anchorName);
    }

    public MatchActorType getActorType(String actorName) {
        initialize();
        return actorNameToType.get(actorName);
    }

    public MatchActorType getActorTypeByNameAbbr(String actorNameAbbr) {
        initialize();
        return actorNameAbbrToType.get(actorNameAbbr);
    }

    private void initialize() {
        if (system == null) {
            synchronized (this) {
                if (system == null) {
                    system = ActorSystemFactory.create("datacloud", 16);
                    initDefaultDataSourceThreadPool();
                    initActors();
                    initActorInfo();
                }
            }
        }
    }

    private void initActors() {
        initAnchors();
        initMicroEngines();
        initJunctions();
        initDataSourceActors();
        initMetricActors();

        log.info("All match actors created.");
    }

    private void initAnchors() {
        List<String> anchors = matchDecisionGraphService.findAllAnchors();
        anchors.forEach(anchor -> {
            initNamedActor(FuzzyMatchAnchorActor.class, false, 1,
                    MatchActorUtils.getFullActorName(anchor, MatchActorType.ANCHOR));
        });
    }

    private void initMicroEngines() {
        initNamedActor(DunsDomainBasedMicroEngineActor.class);
        initNamedActor(DomainBasedMicroEngineActor.class);
        initNamedActor(DunsBasedMicroEngineActor.class);
        initNamedActor(LocationToDunsMicroEngineActor.class);
        initNamedActor(LocationToCachedDunsMicroEngineActor.class);
        initNamedActor(DunsValidateMicroEngineActor.class);
        initNamedActor(CachedDunsValidateMicroEngineActor.class);
        initNamedActor(DunsGuideValidateMicroEngineActor.class);
        initNamedActor(CachedDunsGuideValidateMicroEngineActor.class);
        initNamedActor(DomainCountryZipCodeBasedMicroEngineActor.class);
        initNamedActor(DomainCountryStateBasedMicroEngineActor.class);
        initNamedActor(DomainCountryBasedMicroEngineActor.class);
        initNamedActor(EntityIdAssociateMicroEngineActor.class);
        initNamedActor(EntityEmailBasedMicroEngineActor.class);
        initNamedActor(EntityNameBasedMicroEngineActor.class);
        initNamedActor(EntitySystemIdBasedMicroEngineActor.class);
        initNamedActor(EntityDomainBasedMicroEngineActor.class);
    }

    private void initDataSourceActors() {
        initNamedActor(DynamoLookupActor.class, true, dynamoLookupActorCardinality);
        initNamedActor(DnbLookupActor.class, true, dnbLookupActorCardinality);
        initNamedActor(DnBCacheLookupActor.class, true, dnbCacheLookupActorCardinality);
        initNamedActor(DunsGuideBookLookupActor.class, true, dunsGuideBookLookupActorCardinality);
        initNamedActor(CDLLookupActor.class, true, cdlLookupActorCardinality);
        initNamedActor(CDLAssociateActor.class, true, cdlAssociateActorCardinality);
    }

    private void initJunctions() {
        initNamedActor(FuzzyMatchJunctionActor.class);
        initNamedActor(AccountMatchJunctionActor.class);
    }

    private void initMetricActors() {
        initNamedActor(MetricActor.class, true, metricActorCardinality);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz) {
        return initNamedActor(actorClz, false, 1);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting,
            int routingCardinality) {
        return initNamedActor(actorClz, useRouting, routingCardinality, actorClz.getSimpleName());
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting,
            int routingCardinality, String actorName) {
        ActorRef actorRef;
        if (useRouting) {
            actorRef = actorFactory.create(system, actorName, actorClz,
                    RoutingLogic.RoundRobinRoutingLogic, routingCardinality);
        } else {
            actorRef = actorFactory.create(system, actorName, actorClz);
        }
        actorRefMap.put(actorName, actorRef);
        actorPathMap.putIfAbsent(ActorUtils.getPath(actorRef), actorName);
        log.info(String.format("Add actor-ref %s with class %s", actorName, actorClz.getSimpleName()));
        return actorRef;
    }

    private void initDefaultDataSourceThreadPool() {
        log.info("Initialize default data source thread pool.");
        BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
        dataSourceServiceExecutor = new ThreadPoolExecutor(defaultThreadpoolCountMin, defaultThreadpoolCountMax, 1,
                TimeUnit.MINUTES, runnableQueue);
    }

    private void initActorInfo() {
        log.info("Initialze anchor path info");
        List<String> anchors = matchDecisionGraphService.findAllAnchors();
        anchors.forEach(anchor -> {
            anchorPaths.put(anchor, ActorUtils.getPath(getActorRef(anchor)));
        });

        initActorType();
    }

    // TODO: Change to dynamically load from DecisionGraph table instead of
    // hard-coding
    private void initActorType() {
        Object[][] types = new Object[][] {
                { DunsDomainBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DomainBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DunsBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { LocationToDunsMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { LocationToCachedDunsMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DunsValidateMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { CachedDunsValidateMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DunsGuideValidateMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { CachedDunsGuideValidateMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DomainCountryZipCodeBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DomainCountryStateBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { DomainCountryBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { EntityIdAssociateMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { EntityEmailBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { EntityNameBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //
                { EntityDomainBasedMicroEngineActor.class.getSimpleName(), MatchActorType.MICRO_ENGINE }, //

                { FuzzyMatchJunctionActor.class.getSimpleName(), MatchActorType.JUNCION }, //
                { AccountMatchJunctionActor.class.getSimpleName(), MatchActorType.JUNCION }, //
        };
        Stream.of(types).forEach(type -> {
            actorNameToType.put((String) type[0], (MatchActorType) type[1]);
            actorNameAbbrToType.put(MatchActorUtils.getShortActorName((String) type[0], (MatchActorType) type[1]),
                    (MatchActorType) type[1]);
        });
    }
}
