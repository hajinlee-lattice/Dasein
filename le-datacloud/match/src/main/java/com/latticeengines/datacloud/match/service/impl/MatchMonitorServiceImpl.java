package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DnBLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DynamoDBLookupService;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.exposed.service.MatchMonitorService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

public class MatchMonitorServiceImpl implements MatchMonitorService {
    private static final Logger log = LoggerFactory.getLogger(MatchMonitorServiceImpl.class);

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private DnBLookupService dnbLookupService;

    @Autowired
    @Resource(name = "dnBLookupService")
    private DataSourceLookupService dnbDataSourceLookupService;

    @Autowired
    private DynamoDBLookupService dynamoDBLookupService;

    @Autowired
    @Resource(name = "dynamoDBLookupService")
    private DataSourceLookupService dynamoDataSourceLookupService;

    @Autowired
    private DomainCollectService domainCollectService;

    // Service in other proj will push metric here
    private ConcurrentMap<String, String> externalMetrics = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("commonTaskScheduler")
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
        sb.append(String.format("total memory: %d\n", runtime.totalMemory()));
        sb.append(String.format("free memory: %d\n", runtime.freeMemory()));
        if (actorSystem.isBatchMode()) {
            // DnBLookupService Checking
            Map<String, Integer> dnbUnsubmittedStats = dnbLookupService.getUnsubmittedStats();
            Map<String, Integer> dnbSubmittedStats = dnbLookupService.getSubmittedStats();
            Map<String, Integer> dnbFinishedStats = dnbLookupService.getFinishedStats();
            Map<String, Integer> dnbTotalStats = dnbDataSourceLookupService.getTotalPendingReqStats();
            sb.append("DNB BATCH\n");
            sb.append(String.format("unsubmitted records in queue: %d\n",
                    dnbUnsubmittedStats.get(MatchConstants.REQUEST_NUM)));
            sb.append(String.format("submitted records in queue: %d (batches: %d)\n",
                    dnbSubmittedStats.get(MatchConstants.REQUEST_NUM),
                    dnbSubmittedStats.get(MatchConstants.BATCH_NUM)));
            sb.append(String.format("finished records in queue: %d (batches: %d)\n",
                    dnbFinishedStats.get(MatchConstants.REQUEST_NUM), dnbFinishedStats.get(MatchConstants.BATCH_NUM)));
            sb.append(String.format(
                    "total accepted records (include records removed from queues but still being processed): %d\n",
                    dnbTotalStats.get(MatchConstants.REQUEST_NUM)));
            sb.append(String.format("total cached return addrs: %d\n", dnbTotalStats.get(MatchConstants.ADDRESS_NUM)));
        } else {
            // DnBLookupService Checking
            Map<String, Integer> dnbTotalStats = dnbDataSourceLookupService.getTotalPendingReqStats();
            sb.append("DNB REALTIME\n");
            sb.append(String.format(
                    "total accepted records (include records removed from queues but still being processed): %d\n",
                    dnbTotalStats.get(MatchConstants.REQUEST_NUM)));
            sb.append(String.format("total cached return addrs: %d\n", dnbTotalStats.get(MatchConstants.ADDRESS_NUM)));

        }
        // DynamoLookupService Checking
        Map<String, Integer> dynamoPendingStats = dynamoDBLookupService.getPendingReqStats();
        Map<String, Integer> dynamoTotalStats = dynamoDataSourceLookupService.getTotalPendingReqStats();
        sb.append("DYNAMO FETCH\n");
        sb.append(String.format("pending records in queue: %d\n", dynamoPendingStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format(
                "total accepted records (include records removed from queues but still being processed): %d\n",
                dynamoTotalStats.get(MatchConstants.REQUEST_NUM)));
        sb.append(String.format("total cached return addrs: %d\n", dynamoTotalStats.get(MatchConstants.ADDRESS_NUM)));
        // DomainCollectService Checking
        sb.append("DOMAIN COLLECT\n");
        sb.append(String.format("pending domains in queue: %d\n", domainCollectService.getQueueSize()));
        // DnBCacheService Checking
        sb.append("DNB CACHE DUMP\n");
        sb.append(String.format("pending DnBCaches in queue: %d\n", dnbCacheService.getQueueSize()));
        // External Services Checking
        synchronized (externalMetrics) {
            for (String service : externalMetrics.keySet()) {
                sb.append(service);
                sb.append(externalMetrics.get(service));
            }
        }
        log.info(sb.toString());
    }

    @Override
    public void precheck(String matchVersion) {
        checkDataFabricEntityMgr(matchVersion);
    }

    private void checkDataFabricEntityMgr(String matchVersion) {
        checkDnBCacheEntityMgr(matchVersion);
        checkAccountLookupEntityMgr(matchVersion);
    }

    private void checkDnBCacheEntityMgr(String matchVersion) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(matchVersion)) {
            DnBCacheEntityMgr entityMgr = dnbCacheService.getCacheMgr();
            if (entityMgr == null || entityMgr.isDisabled()) {
                throw new RuntimeException("DnBCacheEntityMgr is disabled.");
            }
        }
    }

    private void checkAccountLookupEntityMgr(String matchVersion) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(matchVersion)) {
            AccountLookupEntryMgr lookupEntityMgr = accountLookupService.getLookupMgr(matchVersion);
            if (lookupEntityMgr == null || lookupEntityMgr.isDisabled()) {
                throw new RuntimeException("AccountLookupEntryMgr is disabled.");
            }
            LatticeAccountMgr latticeAccountMgr = accountLookupService.getAccountMgr(matchVersion);
            if (latticeAccountMgr == null || latticeAccountMgr.isDisabled()) {
                throw new RuntimeException("LatticeAccountMgr is disabled.");
            }
        }
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
