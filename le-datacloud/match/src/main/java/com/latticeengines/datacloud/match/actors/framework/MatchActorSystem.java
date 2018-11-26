package com.latticeengines.datacloud.match.actors.framework;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import com.latticeengines.datacloud.match.actors.visitor.impl.CDLAssociateActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CDLLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideBookLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsValidateMicroEngineActor;
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
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnBCacheLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnbLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryStateBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryZipCodeBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.FuzzyMatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToCachedDunsMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;

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

    private ConcurrentMap<String, ActorRef> actorRefMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, String> actorPathMap = new ConcurrentHashMap<>();

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

    ActorRef getActorRef(String actorClassName) {
        return getActorRefMap().get(actorClassName);
    }

    String getActorName(String actorPath) {
        return getActorPathMap().get(actorPath);
    }

    public String getActorName(ActorRef actorRef) {
        String path = actorRef.path().toSerializationFormat();
        String clzName = getActorName(path);
        if (StringUtils.isEmpty(clzName)) {
            return path;
        } else {
            return clzName;
        }
    }

    ActorRef getFuzzyMatchAnchor() {
        return getActorRef(FuzzyMatchAnchorActor.class);
    }

    public ActorRef getMetricActor() {
        initilize();
        return getActorRef(MetricActor.class);
    }

    public Future<Object> askAnchor(MatchTraveler traveler, Timeout timeout) {
        initilize();
        return Patterns.ask(getFuzzyMatchAnchor(), traveler, timeout);
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
        initilize();
        return system;
    }

    private ConcurrentMap<String, ActorRef> getActorRefMap() {
        initilize();
        return actorRefMap;
    }

    private ConcurrentMap<String, String> getActorPathMap() {
        initilize();
        return actorPathMap;
    }

    private void initilize() {
        if (system == null) {
            synchronized (this) {
                if (system == null) {
                    system = ActorSystemFactory.create("datacloud", 16);
                    initDefaultDataSourceThreadPool();
                    initActors();
                }
            }
        }
    }

    private void initActors() {
        initNamedActor(DynamoLookupActor.class, true, dynamoLookupActorCardinality);
        initNamedActor(DnbLookupActor.class, true, dnbLookupActorCardinality);
        initNamedActor(DnBCacheLookupActor.class, true, dnbCacheLookupActorCardinality);
        initNamedActor(DunsGuideBookLookupActor.class, true, dunsGuideBookLookupActorCardinality);
        initNamedActor(CDLLookupActor.class, true, cdlLookupActorCardinality);
        initNamedActor(CDLAssociateActor.class, true, cdlAssociateActorCardinality);

        initMicroEngines();

        initNamedActor(FuzzyMatchAnchorActor.class);

        initNamedActor(MetricActor.class, true, metricActorCardinality);

        log.info("All match actors created.");
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
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz) {
        return initNamedActor(actorClz, false, 1);
    }

    private <T extends ActorTemplate> ActorRef initNamedActor(Class<T> actorClz, boolean useRouting,
            int routingCardinality) {
        ActorRef actorRef;
        if (useRouting) {
            actorRef = actorFactory.create(system, actorClz.getSimpleName(), actorClz,
                    RoutingLogic.RoundRobinRoutingLogic, routingCardinality);
        } else {
            actorRef = actorFactory.create(system, actorClz.getSimpleName(), actorClz);
        }
        actorRefMap.put(actorClz.getSimpleName(), actorRef);
        actorPathMap.putIfAbsent(actorRef.path().toSerializationFormat(), actorClz.getSimpleName());
        log.info("Add actor-ref " + actorClz.getSimpleName());
        return actorRef;
    }

    private void initDefaultDataSourceThreadPool() {
        log.info("Initialize default data source thread pool.");
        BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
        dataSourceServiceExecutor = new ThreadPoolExecutor(defaultThreadpoolCountMin, defaultThreadpoolCountMax, 1,
                TimeUnit.MINUTES, runnableQueue);
    }
}
