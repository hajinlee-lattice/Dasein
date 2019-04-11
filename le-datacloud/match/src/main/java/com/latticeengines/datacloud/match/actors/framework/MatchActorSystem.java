package com.latticeengines.datacloud.match.actors.framework;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.ActorFactory;
import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.MetricActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.AccountMatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.ContactMatchPlannerMicroEngineActor;
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
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityAssociateActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailAIDBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdResolveMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityLookupActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNamePhoneAIDBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNamePhoneBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToCachedDunsMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchAnchorActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchJunctionActor;
import com.latticeengines.domain.exposed.actors.ActorType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

@Component("matchActorSystem")
public class MatchActorSystem extends ActorSystemTemplate {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystem.class);

    public static final String BATCH_MODE = "batch";
    public static final String REALTIME_MODE = "realtime";

    private static final int MAX_ALLOWED_RECORD_COUNT_SYNC = DataCloudConstants.REAL_TIME_MATCH_RECORD_LIMIT;
    private static final int MAX_ALLOWED_RECORD_COUNT_ASYNC = 10000;
    private final AtomicInteger maxAllowedRecordCount = new AtomicInteger(MAX_ALLOWED_RECORD_COUNT_SYNC);

    private boolean batchMode = false;

    @Value("${datacloud.match.dnbLookupActor.actor.cardinality:3}")
    private int dnbLookupActorCardinality;

    @Value("${datacloud.match.dnbCacheLookupActor.actor.cardinality:3}")
    private int dnbCacheLookupActorCardinality;

    @Value("${datacloud.match.dynamoLookupActor.actor.cardinality:10}")
    private int dynamoLookupActorCardinality;

    @Value("${datacloud.match.dunsGuideBookLookupActor.actor.cardinality:3}")
    private int dunsGuideBookLookupActorCardinality;

    @Value("${datacloud.match.entityLookupActor.actor.cardinality:3}")
    private int entityLookupActorCardinality;

    @Value("${datacloud.match.entityAssociateActor.actor.cardinality:2}")
    private int entityAssociateActorCardinality;

    @Value("${datacloud.match.metricActor.actor.cardinality:4}")
    private int metricActorCardinality;

    @Value("${datacloud.match.actor.datasource.default.threadpool.count.min}")
    private Integer defaultThreadpoolCountMin;

    @Value("${datacloud.match.actor.datasource.default.threadpool.count.max}")
    private Integer defaultThreadpoolCountMax;

    @Value("${datacloud.match.actor.datasource.default.threadpool.queue.size}")
    private Integer defaultThreadpoolQueueSize;

    private ExecutorService dataSourceServiceExecutor;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    /****************************
     * Initialization & Destroy
     ****************************/

    @Autowired
    public MatchActorSystem(ActorFactory actorFactory) {
        this.actorFactory = actorFactory;
    }

    @PreDestroy
    private void predestroy() {
        log.info("Shutting down match actor system");
        try {
            if (dataSourceServiceExecutor != null) {
                dataSourceServiceExecutor.shutdownNow();
                dataSourceServiceExecutor.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            if (system != null) {
                system.shutdown();
                system.awaitTermination(Duration.create(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            }
            log.info("Completed shutdown of match actor system");
        } catch (Exception e) {
            log.error("Fail to finish all pre-destroy actions", e);
        }
    }

    @Override
    public ActorRef getAnchor() {
        return getActorRef(MatchAnchorActor.class);
    }

    @Override
    protected void initAnchors() {
        initNamedActor(MatchAnchorActor.class);
        actorNameToType.put(MatchAnchorActor.class.getSimpleName(), ActorType.ANCHOR);
        actorNameAbbrToType.put(
                MatchActorUtils.getShortActorName(MatchAnchorActor.class.getSimpleName(), ActorType.ANCHOR),
                ActorType.ANCHOR);
    }

    @Override
    protected void initJunctions() {
        List<DecisionGraph> decisionGraphs = matchDecisionGraphService.findAll();
        Set<String> junctionAbbrs = new HashSet<>();
        decisionGraphs.forEach(dg -> {
            junctionAbbrs.addAll(dg.getJunctionGraphMap().keySet());
        });

        junctionAbbrs.forEach(junctionAbbr -> {
            String junctionName = MatchActorUtils.getFullActorName(junctionAbbr, ActorType.JUNCION);
            initNamedActor(MatchJunctionActor.class, false, 1, junctionName);
            actorNameToType.put(junctionName, ActorType.JUNCION);
            actorNameAbbrToType.put(junctionAbbr, ActorType.JUNCION);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void initMicroEngines() {
        Class<? extends ActorTemplate>[] microEngineClz = new Class[] { //
                AccountMatchPlannerMicroEngineActor.class, //
                DunsDomainBasedMicroEngineActor.class, //
                DomainBasedMicroEngineActor.class, //
                DunsBasedMicroEngineActor.class, //
                LocationToDunsMicroEngineActor.class, //
                LocationToCachedDunsMicroEngineActor.class, //
                DunsValidateMicroEngineActor.class, //
                CachedDunsValidateMicroEngineActor.class, //
                DunsGuideValidateMicroEngineActor.class, //
                CachedDunsGuideValidateMicroEngineActor.class, //
                DomainCountryZipCodeBasedMicroEngineActor.class, //
                DomainCountryStateBasedMicroEngineActor.class, //
                DomainCountryBasedMicroEngineActor.class, //
                EntityIdAssociateMicroEngineActor.class, //
                EntityIdResolveMicroEngineActor.class,
                EntityNameCountryBasedMicroEngineActor.class, //
                EntitySystemIdBasedMicroEngineActor.class, //
                EntityDomainCountryBasedMicroEngineActor.class, //
                EntityDunsBasedMicroEngineActor.class, //
                ContactMatchPlannerMicroEngineActor.class, //
                EntityEmailAIDBasedMicroEngineActor.class, //
                EntityNamePhoneAIDBasedMicroEngineActor.class, //
                EntityEmailBasedMicroEngineActor.class, //
                EntityNamePhoneBasedMicroEngineActor.class, //
        };
        for (Class<? extends ActorTemplate> clz : microEngineClz) {
            initNamedActor(clz);
            actorNameToType.put(clz.getSimpleName(), ActorType.MICRO_ENGINE);
            actorNameAbbrToType.put(MatchActorUtils.getShortActorName(clz.getSimpleName(), ActorType.MICRO_ENGINE),
                    ActorType.MICRO_ENGINE);
        }
    }

    @Override
    protected void initMetricActors() {
        initNamedActor(MetricActor.class, true, metricActorCardinality);
    }

    @Override
    protected void initAssistantActors() {
        initDataSourceActors();
    }

    private void initDataSourceActors() {
        initNamedActor(DynamoLookupActor.class, true, dynamoLookupActorCardinality);
        initNamedActor(DnbLookupActor.class, true, dnbLookupActorCardinality);
        initNamedActor(DnBCacheLookupActor.class, true, dnbCacheLookupActorCardinality);
        initNamedActor(DunsGuideBookLookupActor.class, true, dunsGuideBookLookupActorCardinality);
        initNamedActor(EntityLookupActor.class, true, entityLookupActorCardinality);
        initNamedActor(EntityAssociateActor.class, true, entityAssociateActorCardinality);
    }

    @Override
    protected void postInitialize() {
        initDefaultDataSourceThreadPool();
    }

    private void initDefaultDataSourceThreadPool() {
        log.info("Initialize default data source thread pool.");
        BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
        dataSourceServiceExecutor = new ThreadPoolExecutor(defaultThreadpoolCountMin, defaultThreadpoolCountMax, 1,
                TimeUnit.MINUTES, runnableQueue);
    }

    /****************************
     * System settings
     ****************************/

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

}
