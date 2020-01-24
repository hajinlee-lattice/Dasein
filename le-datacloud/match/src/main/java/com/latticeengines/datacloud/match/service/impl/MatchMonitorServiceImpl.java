package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DnBLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DynamoDBLookupService;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.exposed.service.MatchMonitorService;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

public class MatchMonitorServiceImpl implements MatchMonitorService {
    private static final Logger log = LoggerFactory.getLogger(MatchMonitorServiceImpl.class);

    @Inject
    private DnBCacheService dnbCacheService;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private DnBLookupService dnbLookupService;

    @Resource(name = "dnbLookupService")
    private DataSourceLookupService dnbDataSourceLookupService;

    @Resource(name = "dnbCacheLookupService")
    private DataSourceLookupService dnbCacheDataSourceLookupService;

    @Inject
    private DynamoDBLookupService dynamoDBLookupService;

    @Resource(name = "dynamoDBLookupService")
    private DataSourceLookupService dynamoDataSourceLookupService;

    @Inject
    private DomainCollectService domainCollectService;

    // Service in other proj will push metric here
    private ConcurrentMap<String, String> externalMetrics = new ConcurrentHashMap<>();

    @Resource(name = "commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        scheduler.scheduleWithFixedDelay(this::monitor,
                new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2)), TimeUnit.MINUTES.toMillis(2));
    }

    @Override
    public synchronized void monitor() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n---------------- Match Service Status ----------------\n");

        // Memory Checking
        sb.append("RUNTIME MEMORY\n");
        Runtime runtime = Runtime.getRuntime();
        sb.append(String.format("Total memory: %d\n", runtime.totalMemory()));
        sb.append(String.format("Free memory: %d\n", runtime.freeMemory()));

        // DnBLookupService Checking
        if (actorSystem.isBatchMode()) {
            Map<String, Integer> dnbUnsubmittedStats = dnbLookupService.getUnsubmittedStats();
            Map<String, Integer> dnbSubmittedStats = dnbLookupService.getSubmittedStats();
            Map<String, Integer> dnbFinishedStats = dnbLookupService.getFinishedStats();

            sb.append("DNB BATCH LOOKUP\n");
            sb.append(String.format("DnBLookupService - unsubmitted records in batch req queue: %d\n",
                    dnbUnsubmittedStats.get(MatchConstants.REQUEST_NUM)));
            sb.append(String.format("DnBLookupService - submitted records in batch req queue: %d (batches: %d)\n",
                    dnbSubmittedStats.get(MatchConstants.REQUEST_NUM),
                    dnbSubmittedStats.get(MatchConstants.BATCH_NUM)));
            sb.append(String.format("DnBLookupService - finished records in batch req queue: %d (batches: %d)\n",
                    dnbFinishedStats.get(MatchConstants.REQUEST_NUM), dnbFinishedStats.get(MatchConstants.BATCH_NUM)));

        } else {
            sb.append("DNB REALTIME LOOKUP\n");
            Map<String, Integer> dnbRealtimeStats = dnbLookupService.getRealtimeReqStats();
            sb.append(String.format("DnBLookupService - active records in threadpool: %d\n",
                    dnbRealtimeStats.get(MatchConstants.ACTIVE_REQ_NUM)));
            sb.append(String.format("DnBLookupService - queued records in threadpool: %d\n",
                    dnbRealtimeStats.get(MatchConstants.QUEUED_REQ_NUM)));
        }
        Map<String, Integer> dnbTotalStats = dnbDataSourceLookupService.getTotalPendingReqStats();
        sb.append(String.format("DnBLookupActor - total accepted records: %d\n",
                dnbTotalStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format("DnBLookupActor - total cached return addrs: %d\n",
                dnbTotalStats.get(MatchConstants.ADDRESS_NUM)));
        sb.append(String.format("DnBLookupActor - active records in threadpool: %d\n",
                dnbTotalStats.get(MatchConstants.ACTIVE_REQ_NUM)));
        sb.append(String.format("DnBLookupActor - queued records in threadpool: %d\n",
                dnbTotalStats.get(MatchConstants.QUEUED_REQ_NUM)));

        // DnBCacheLookupService Checking
        sb.append("DNB CACHE LOOKUP\n");
        Map<String, Integer> dnbCacheTotalStats = dnbCacheDataSourceLookupService.getTotalPendingReqStats();
        sb.append(String.format("DnBCacheLookupActor - total accepted records: %d\n",
                dnbCacheTotalStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format("DnBCacheLookupActor - total cached return addrs: %d\n",
                dnbCacheTotalStats.get(MatchConstants.ADDRESS_NUM)));
        sb.append(String.format("DnBCacheLookupActor - active records in threadpool: %d\n",
                dnbCacheTotalStats.get(MatchConstants.ACTIVE_REQ_NUM)));
        sb.append(String.format("DnBCacheLookupActor - queued records in threadpool: %d\n",
                dnbCacheTotalStats.get(MatchConstants.QUEUED_REQ_NUM)));

        // DynamoLookupService Checking
        Map<String, Integer> dynamoPendingStats = dynamoDBLookupService.getPendingReqStats();
        Map<String, Integer> dynamoTotalStats = dynamoDataSourceLookupService.getTotalPendingReqStats();
        sb.append("DYNAMO LOOKUP\n");
        sb.append(String.format("DynamoDBLookupService - pending records in queue: %d\n",
                dynamoPendingStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format("DynamoDBLookupActor - total accepted records: %d\n",
                dynamoTotalStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format("DynamoDBLookupActor - total cached return addrs: %d\n",
                dynamoTotalStats.get(MatchConstants.ADDRESS_NUM)));
        sb.append(String.format("DynamoDBLookupActor - active records in threadpool: %d\n",
                dynamoTotalStats.get(MatchConstants.ACTIVE_REQ_NUM)));
        sb.append(String.format("DynamoDBLookupActor - queued records in threadpool: %d\n",
                dynamoTotalStats.get(MatchConstants.QUEUED_REQ_NUM)));

        // DomainCollectService Checking
        sb.append("DOMAIN COLLECT\n");
        sb.append(String.format("Pending domains in queue: %d\n", domainCollectService.getQueueSize()));

        // DnBCacheService Checking
        sb.append("DNB CACHE DUMP\n");
        sb.append(String.format("Pending DnBCaches in queue: %d\n", dnbCacheService.getQueueSize()));

        // External Services Checking
        synchronized (externalMetrics) {
            for (String service : externalMetrics.keySet()) {
                sb.append(service);
                sb.append(externalMetrics.get(service));
            }
        }
        log.info(sb.toString());
    }

    public void pushMetrics(String service, String message) {
        if (StringUtils.isBlank(service) || StringUtils.isBlank(message)) {
            return;
        }
        service = service.toUpperCase();
        if (!StringUtils.endsWith(service, "\n")) {
            service += "\n";
        }
        if (!StringUtils.endsWith(message, "\n")) {
            message += "\n";
        }
        externalMetrics.put(service, message);
    }
}
