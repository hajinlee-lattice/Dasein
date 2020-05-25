package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.ActionStatService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.scheduling.ActionStat;
import com.latticeengines.domain.exposed.cdl.scheduling.GreedyScheduler;
import com.latticeengines.domain.exposed.cdl.scheduling.PASchedulerConfig;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPATimeClock;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAUtil;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingResult;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("SchedulingPAService")
public class SchedulingPAServiceImpl implements SchedulingPAService {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAServiceImpl.class);

    private static final String SYSTEM_STATUS = "SYSTEM_STATUS";
    private static final String TENANT_ACTIVITY_LIST = "TENANT_ACTIVITY_LIST";
    private static final String SCHEDULING_GROUP_SUFFIX = "_scheduling";
    private static final String DEFAULT_SCHEDULING_GROUP = "Default";
    private static final String PA_JOB_TYPE = "processAnalyzeWorkflow";
    private static final String USER_ERROR_CATEGORY = "User Error";

    private static ObjectMapper om = new ObjectMapper();

    private static final Set<String> TEST_TENANT_PREFIX = Sets.newHashSet("LETest", "letest",
            "ScoringServiceImplDeploymentTestNG", "RTSBulkScoreWorkflowDeploymentTestNG",
            "CDLComponentDeploymentTestNG");

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    @Lazy
    @Inject
    private ActionStatService actionStatService;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Value("${cdl.processAnalyze.maximum.priority.large.account.count}")
    private long largeAccountCountLimit;

    @Value("${cdl.processAnalyze.maximum.priority.large.transaction.count}")
    private long largeTransactionCountLimit;

    @Value("${cdl.processAnalyze.job.retry.count:1}")
    private int processAnalyzeJobRetryCount;

    @Value("${cdl.processAnalyze.maximum.priority.schedulenow.job.count}")
    private int maxScheduleNowJobCount;

    @Value("${cdl.processAnalyze.maximum.priority.large.job.count}")
    private int maxLargeJobCount;

    @Value("${cdl.processAnalyze.maximum.priority.large.txn.job.count}")
    private int maxLargeTxnJobCount;

    @Value("${cdl.processAnalyze.concurrent.job.count}")
    private int concurrentProcessAnalyzeJobs;

    @Value("${cdl.processAnalyze.retry.expired.time}")
    private long retryExpiredTime;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${cdl.processAnalyze.job.autoschedule.failcount:3}")
    private int autoScheduleMaxFailCount;

    @Value("${cdl.processAnalyze.job.datacloudrefresh.failcount:3}")
    private int dataCloudRefreshMaxFailCount;

    private TimeClock schedulingPATimeClock = new SchedulingPATimeClock();

    private List<Long> jobQueryTime = new ArrayList<>();

    @Override
    public Map<String, Object> setSystemStatus(@NotNull String schedulerName) {

        int runningTotalCount = 0;
        int runningScheduleNowCount = 0;
        int runningLargeJobCount = 0;
        int runningLargeTxnJobCount = 0;

        Set<String> largeJobTenantId = new HashSet<>();
        Set<String> runningPATenantId = new HashSet<>();
        Set<String> largeTransactionTenantId = new HashSet<>();

        setSchedulerQuotaLimit();

        List<TenantActivity> tenantActivityList = new LinkedList<>();
        String schedulingGroup = getSchedulingGroup(schedulerName);
        List<DataFeed> allDataFeeds = dataFeedService.getDataFeedsBySchedulingGroup(TenantStatus.ACTIVE, "4.0", schedulingGroup);
        log.info(String.format("DataFeed for active tenant count: %d.", allDataFeeds.size()));
        String currentBuildNumber = columnMetadataProxy.latestBuildNumber();
        log.debug(String.format("Current build number is : %s.", currentBuildNumber));

        Map<Long, ActionStat> actionStats = getActionStats();
        Set<String> largeTenantExemptionList = getLargeTenantExemptionSet();
        Map<String, Long> paFailedMap = getPASubmitFailedRedisMap();
        Set<Long> migrationTenantPids = getMigrationTenantPids();

        Set<String> skippedTestTenants = new HashSet<>();
        Set<String> skippedMigrationTenants = new HashSet<>();
        log.info("Number of tenant with new actions after last PA = {}", actionStats.size());
        log.debug("Action stats = {}, large tenant exemption list = {}", actionStats, largeTenantExemptionList);


        for (DataFeed simpleDataFeed : allDataFeeds) {
            if (simpleDataFeed.getTenant() == null) {
                // check just in case
                continue;
            }
            if (isTestTenant(simpleDataFeed)) {
                // not scheduling for test tenants
                skippedTestTenants.add(simpleDataFeed.getTenant().getId());
                continue;
            }
            if (isMigrationTenant(simpleDataFeed.getTenant(), migrationTenantPids)) {
                // skip entity match migration tenants
                skippedMigrationTenants.add(simpleDataFeed.getTenant().getId());
                continue;
            }

            // configure the context
            Tenant tenant = simpleDataFeed.getTenant();
            MultiTenantContext.setTenant(tenant);
            String tenantId = tenant.getId();

            // retrieve data collection status
            DataCollectionStatus dcStatus = null;
            try {
                dcStatus = dataCollectionService.getOrCreateDataCollectionStatus(tenantId, null);
            } catch (Exception e) {
                log.error("Failed to get or create data collection status for tenant {}", tenantId);
            }

            if (simpleDataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                runningTotalCount++;
                runningPATenantId.add(tenantId);
                if (simpleDataFeed.isScheduleNow()) {
                    runningScheduleNowCount++;
                }
                if (isLarge(tenantId, largeTenantExemptionList, dcStatus) || isLargeTransaction(tenantId,
                        largeTenantExemptionList, dcStatus)) {
                    runningLargeJobCount++;
                }
                if (isLargeTransaction(tenantId, largeTenantExemptionList, dcStatus)) {
                    runningLargeTxnJobCount++;
                }
            } else if (!DataFeed.Status.RUNNING_STATUS.contains(simpleDataFeed.getStatus())) {
                if (paFailedMap.containsKey(tenantId)) {
                    continue;
                }
                TenantActivity tenantActivity = new TenantActivity();
                tenantActivity.setTenantId(tenantId);
                tenantActivity.setTenantType(tenant.getTenantType());
                tenantActivity.setLarge(isLarge(tenantId, largeTenantExemptionList, dcStatus)
                        || isLargeTransaction(tenantId, largeTenantExemptionList, dcStatus));
                tenantActivity.setLargeTransaction(isLargeTransaction(tenantId, largeTenantExemptionList, dcStatus));
                if (tenantActivity.isLargeTransaction()) {
                    largeTransactionTenantId.add(tenantId);
                }
                if (tenantActivity.isLarge()) {
                    largeJobTenantId.add(tenantId);
                }
                DataFeedExecution execution;
                try {
                    execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(simpleDataFeed,
                            DataFeedExecutionJobType.PA);
                } catch (Exception e) {
                    log.warn("cannot find execution, dataFeedPid is {}, tenantId is {}.", simpleDataFeed.getPid(),
                            tenantId);
                    execution = null;
                }
                tenantActivity.setRetry(retryValidation(execution, tenantId));
                if (execution != null && execution.getUpdated() != null) {
                    tenantActivity.setLastFinishTime(execution.getUpdated().getTime());
                }
                tenantActivity.setScheduledNow(simpleDataFeed.isScheduleNow());
                tenantActivity.setScheduleTime(
                        tenantActivity.isScheduledNow() ? simpleDataFeed.getScheduleTime().getTime() : null);

                // auto scheduling
                if (actionStats.containsKey(tenant.getPid())) {
                    Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, execution);
                    if (invokeTime != null) {
                        if (simpleDataFeed.getNextInvokeTime() == null
                                || !simpleDataFeed.getNextInvokeTime().equals(invokeTime)) {
                            dataFeedService.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                        }
                        tenantActivity.setInvokeTime(invokeTime.getTime());
                        ActionStat stat = actionStats.get(tenant.getPid());
                        if (stat.getFirstActionTime() != null) {
                            tenantActivity.setFirstActionTime(stat.getFirstActionTime().getTime());
                        }
                        if (stat.getLastActionTime() != null) {
                            tenantActivity.setLastActionTime(stat.getLastActionTime().getTime());
                        }
                        tenantActivity.setAutoSchedule(isNewActionAdded(tenantId, stat.getLastActionTime())
                                || !reachFailCountLimit(tenantId, autoScheduleMaxFailCount));
                    }
                }

                // dc refresh
                tenantActivity.setDataCloudRefresh(isDataCloudRefresh(tenant, currentBuildNumber, dcStatus) &&
                        !reachFailCountLimit(tenantId, dataCloudRefreshMaxFailCount));
                // add to list
                tenantActivityList.add(tenantActivity);
            }
        }

        log.debug("Skipped test tenants = {}", skippedTestTenants);
        // print all for migration tenants, shouldn't be too many at the same time
        log.info("Number of skipped test tenants = {}. Skipped migration tenants = {}", skippedTestTenants.size(),
                skippedMigrationTenants);

        if (CollectionUtils.isNotEmpty(jobQueryTime)) {
            long totalQueryTime = jobQueryTime.stream().mapToLong(Long::longValue).sum();
            log.info("query need retry workflowJob spend {} ms.", totalQueryTime);
        }

        int canRunJobCount = concurrentProcessAnalyzeJobs - runningTotalCount;
        int canRunScheduleNowJobCount = maxScheduleNowJobCount - runningScheduleNowCount;
        int canRunLargeJobCount = maxLargeJobCount - runningLargeJobCount;
        int canRunLargeTxnJobCount = maxLargeTxnJobCount - runningLargeTxnJobCount;

        SystemStatus systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(canRunJobCount);
        systemStatus.setCanRunLargeJobCount(canRunLargeJobCount);
        systemStatus.setCanRunLargeTxnJobCount(canRunLargeTxnJobCount);
        systemStatus.setCanRunScheduleNowJobCount(canRunScheduleNowJobCount);
        systemStatus.setRunningTotalCount(runningTotalCount);
        systemStatus.setRunningLargeJobCount(runningLargeJobCount);
        systemStatus.setRunningLargeTxnJobCount(canRunLargeTxnJobCount);
        systemStatus.setRunningScheduleNowCount(runningScheduleNowCount);
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        systemStatus.setRunningPATenantId(runningPATenantId);
        log.info(
                "There are {} running PAs({} ScheduleNow PAs, {} large PAs, {} large Txn PAs). Tenants = {}. Large PA Tenants = {}, "
                        +
                        "Large Transaction Tenants = {}. schedulerName={}",
                runningTotalCount, runningScheduleNowCount, runningLargeJobCount, runningLargeTxnJobCount,
                runningPATenantId, largeJobTenantId
                , largeTransactionTenantId, schedulerName);
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, systemStatus);
        map.put(TENANT_ACTIVITY_LIST, tenantActivityList);
        return map;
    }

    @Override
    public List<SchedulingPAQueue> initQueue(@NotNull String schedulerName) {
        Map<String, Object> map = setSystemStatus(schedulerName);
        SystemStatus systemStatus = (SystemStatus) map.get(SYSTEM_STATUS);
        List<TenantActivity> tenantActivityList = (List<TenantActivity>) map.get(TENANT_ACTIVITY_LIST);
        return initQueue(systemStatus, tenantActivityList);
    }

    private List<SchedulingPAQueue> initQueue(SystemStatus systemStatus, List<TenantActivity> tenantActivityList) {
        return SchedulingPAUtil.initQueue(schedulingPATimeClock, systemStatus, tenantActivityList);
    }

    @Override
    public SchedulingResult getSchedulingResult(@NotNull String schedulerName) {
        List<SchedulingPAQueue> schedulingPAQueues = initQueue(schedulerName);
        GreedyScheduler greedyScheduler = new GreedyScheduler();
        return greedyScheduler.schedule(schedulingPAQueues);
    }

    @Override
    public Map<String, List<String>> showQueue(@NotNull String schedulerName) {
        List<SchedulingPAQueue> schedulingPAQueues = initQueue(schedulerName);
        Map<String, List<String>> tenantMap = new HashMap<>();
        for (SchedulingPAQueue<?> schedulingPAQueue : schedulingPAQueues) {
            tenantMap.put(schedulingPAQueue.getQueueName(), schedulingPAQueue.getAll());
        }
        log.info("priority Queue : " + JsonUtils.serialize(tenantMap));
        return tenantMap;
    }

    @Override
    public String getPositionFromQueue(@NotNull String schedulerName, String tenantName) {
        List<SchedulingPAQueue> schedulingPAQueues = initQueue(schedulerName);
        for (SchedulingPAQueue<?> schedulingPAQueue : schedulingPAQueues) {
            int index = schedulingPAQueue.getPosition(tenantName);
            if (index != -1) {
                return String.format("tenant %s at Queue %s Position %d", tenantName, schedulingPAQueue.getQueueName(),
                        index);
            }
        }
        return "cannot find this tenant " + tenantName + " in Queue.";
    }

    @Override
    public SchedulingStatus getSchedulingStatus(@NotNull String customerSpace, @NotNull String schedulerName) {
        boolean schedulerEnabled = isSchedulerEnabled(schedulerName);
        DataFeed feed = dataFeedService.getDefaultDataFeed(customerSpace);
        DataFeedExecution execution = null;
        if (feed != null) {
            execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(feed,
                    DataFeedExecutionJobType.PA);
        }
        return new SchedulingStatus(customerSpace, schedulerEnabled, feed, execution,
                retryValidation(execution, customerSpace));
    }

    private boolean isLarge(@NotNull String tenantId, @NotNull Set<String> largeTenantExemptionList,
            DataCollectionStatus status) {
        if (status == null || status.getDetail() == null) {
            return false;
        }
        if (largeTenantExemptionList.contains(CustomerSpace.shortenCustomerSpace(tenantId))) {
            return false;
        }
        DataCollectionStatusDetail detail = status.getDetail();
        return detail.getAccountCount() != null && detail.getAccountCount() > largeAccountCountLimit;
    }

    private boolean isLargeTransaction(@NotNull String tenantId, @NotNull Set<String> largeTenantExemptionList,
                            DataCollectionStatus status) {
        if (status == null || status.getDetail() == null) {
            return false;
        }
        if (largeTenantExemptionList.contains(CustomerSpace.shortenCustomerSpace(tenantId))) {
            return false;
        }
        DataCollectionStatusDetail detail = status.getDetail();
        return detail.getTransactionCount() != null && detail.getTransactionCount() > largeTransactionCountLimit;
    }

    /**
     * Don't retry if last PA failed due to user error or job was canceled
     * will update retryCount in table DataFeedExecution
     */
    private boolean retryValidation(DataFeedExecution execution, String tenantId) {
        try {
            if (execution == null || !DataFeedExecution.Status.Failed.equals(execution.getStatus())
                    || execution.getUpdated() == null || !checkRetryPendingTime(execution.getUpdated().getTime())) {
                log.warn("execution is invalid to retry PA.");
                return false;
            }

            if (reachRetryLimit(CDLJobType.PROCESSANALYZE, execution.getRetryCount())) {
                log.debug("Tenant {} exceeds retry limit and skip failed exeuction",
                        tenantId);
                return false;
            }

            if (execution.getWorkflowId() == null) {
                log.warn("cannot find workflowId, tenant {} cannot be retry.", tenantId);
                return false;
            }
            Job job = getFailedPAJob(execution, tenantId);
            if (USER_ERROR_CATEGORY.equalsIgnoreCase(job.getErrorCategory())) {
                updateRetryCount(execution);
                log.warn("due to user error, tenant {} cannot be retry.", tenantId);
                return false;
            }
            return true;
        } catch (IllegalArgumentException e) {
            updateRetryCount(execution);
            log.error("cannot retry this tenant {}", tenantId, e);
            return false;
        } catch (Exception e) {
            log.error("cannot retry this tenant {}", tenantId, e);
            return false;
        }
    }

    private void updateRetryCount(DataFeedExecution execution) {
        if (execution != null) {
            execution.setRetryCount(processAnalyzeJobRetryCount);
            dataFeedExecutionEntityMgr.updateRetryCount(execution);
        }
    }

    private Job getFailedPAJob(DataFeedExecution execution, String tenantId) {
        long beforeQuery = System.currentTimeMillis();
        Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()), tenantId);
        long queryTime = System.currentTimeMillis() - beforeQuery;
        jobQueryTime.add(queryTime);
        log.info("query workflow {} need {} ms.", execution.getWorkflowId(), queryTime);
        if (job == null || !PA_JOB_TYPE.equalsIgnoreCase(job.getJobType()) || job.getJobStatus() != JobStatus.FAILED) {
            throw new IllegalArgumentException("the last pa job isn't failed in tenant " + tenantId);
        }
        return job;
    }

    private boolean reachRetryLimit(CDLJobType cdlJobType, int retryCount) {
        switch (cdlJobType) {
        case PROCESSANALYZE:
            return retryCount >= processAnalyzeJobRetryCount;
        default:
            return false;
        }
    }

    /*
     * helper from testframework utils. FIXME move the utils to a central location
     */
    private boolean isTestTenant(@NotNull DataFeed feed) {
        if (feed.getTenant() == null || feed.getTenant().getId() == null) {
            return false;
        }

        String tenantId = CustomerSpace.parse(feed.getTenant().getId()).getTenantId();
        boolean findMatch = false;
        for (String prefix : TEST_TENANT_PREFIX) {
            Pattern pattern = Pattern
                    .compile(prefix + "\\d+" + "|" + prefix + "_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_UTC" + "|"
                            + prefix + "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
            Matcher matcher = pattern.matcher(tenantId);
            if (matcher.find()) {
                findMatch = true;
                break;
            }
        }

        return findMatch;
    }

    /*
     * Retrieve all action related stats for scheduling
     */
    private Map<Long, ActionStat> getActionStats() {
        List<ActionStat> ingestActions = actionStatService.getNoOwnerCompletedIngestActionStats();
        List<ActionStat> nonIngestActions = actionStatService.getNoOwnerActionStatsByTypes(getNonIngestAndReplaceActionType());

        return Stream.concat(ingestActions.stream(), nonIngestActions.stream())
                .collect(Collectors.toMap(ActionStat::getTenantPid, stat -> stat, (s1, s2) -> {
                    // merge two stats (compare first/last action time)
                    Date first = s1.getFirstActionTime();
                    if (first == null || (s2.getFirstActionTime() != null && s2.getFirstActionTime().before(first))) {
                        first = s2.getFirstActionTime();
                    }
                    Date last = s1.getLastActionTime();
                    if (last == null || (s2.getLastActionTime() != null && s2.getLastActionTime().after(last))) {
                        last = s2.getLastActionTime();
                    }
                    return new ActionStat(s1.getTenantPid(), first, last);
                }));
    }

    /*
     * helpers for entity match migration
     */
    private Set<Long> getMigrationTenantPids() {
        try {
            return new HashSet<>(migrationTrackEntityMgr.getTenantPidsByStatus(MigrationTrack.Status.STARTED));
        } catch (Exception e) {
            log.error("Failed to retrieve migration tenants", e);
            return Collections.emptySet();
        }
    }

    private boolean isMigrationTenant(@NotNull Tenant tenant, @NotNull Set<Long> migrationTenantPids) {
        return tenant.getPid() != null && migrationTenantPids.contains(tenant.getPid());
    }

    private Date getInvokeTime(DataFeedExecution execution, int invokeHour, Date tenantCreateDate) {
        Calendar calendar = Calendar.getInstance();
        if (execution == null) {
            calendar.setTime(new Date(System.currentTimeMillis()));

            calendar.set(Calendar.HOUR_OF_DAY, invokeHour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            if (calendar.getTime().before(tenantCreateDate)) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
            }
            return calendar.getTime();
        } else if (execution.getStatus() == DataFeedExecution.Status.Started) {
            return null;
        } else {
            if ((execution.getStatus() == DataFeedExecution.Status.Failed)
                    && (execution.getRetryCount() < processAnalyzeJobRetryCount)) {
                calendar.setTime(execution.getUpdated());
                calendar.add(Calendar.MINUTE, 15);
            } else {
                calendar.setTime(execution.getCreated());
                int hour_create = calendar.get(Calendar.HOUR_OF_DAY);
                int day_create = calendar.get(Calendar.DAY_OF_YEAR);

                calendar.set(Calendar.HOUR_OF_DAY, invokeHour);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                if (invokeHour < hour_create) {
                    calendar.add(Calendar.DAY_OF_MONTH, 1);
                }

                int day_invoke = calendar.get(Calendar.DAY_OF_YEAR);
                if ((day_invoke - day_create) * 24 + invokeHour - hour_create < 12) {
                    calendar.add(Calendar.DAY_OF_MONTH, 1);
                }
            }
            return calendar.getTime();
        }

    }

    private Date getNextInvokeTime(CustomerSpace customerSpace, Tenant tenant, DataFeedExecution execution) {
        Date invokeTime = null;

        boolean allowAutoSchedule = false;
        try {
            allowAutoSchedule = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        } catch (Exception e) {
            log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
        }
        if (allowAutoSchedule) {
            log.debug("Tenant {} allow auto scheduling ", customerSpace);
            int invokeHour = zkConfigService.getInvokeTime(customerSpace, CDLComponent.componentName);
            Tenant tenantInContext = MultiTenantContext.getTenant();
            try {
                MultiTenantContext.setTenant(tenant);
                invokeTime = getInvokeTime(execution, invokeHour, new Date(tenant.getRegisteredTime()));
            } finally {
                MultiTenantContext.setTenant(tenantInContext);
            }
        }
        return invokeTime;
    }

    private Boolean checkDataCloudChange(String currentBuildNumber, String customerSpace, DataCollectionStatus status) {
        DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customerSpace);
        List<String> tableNames = dataCollectionService.getTableNames(customerSpace, null,
                TableRoleInCollection.ConsolidatedAccount, activeVersion);
        // try to get the first one if exist
        String accountTableName = CollectionUtils.isEmpty(tableNames) ? null : tableNames.get(0);

        return (status != null
                && (status.getDataCloudBuildNumber() == null
                        || DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber())
                        || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                && StringUtils.isNotBlank(accountTableName));
    }

    private Boolean isDataCloudRefresh(Tenant tenant, String currentBuildNumber, DataCollectionStatus status) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
            boolean allowAutoDataCloudRefresh = batonService.isEnabled(customerSpace,
                    LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY);
            if (allowAutoDataCloudRefresh) {
                return checkDataCloudChange(currentBuildNumber, customerSpace.toString(), status);
            }
        } catch (Exception e) {
            log.error("Unable to check datacloud refresh for tenant {}", tenant.getId(), e);
        }
        return false;
    }

    private boolean checkRetryPendingTime(long lastFinishedTime) {
        return lastFinishedTime - (schedulingPATimeClock.getCurrentTime() - retryExpiredTime * 1000) > 0;
    }

    private String getSchedulingGroup(String schedulerName) {
        try {
            Camille c = CamilleEnvironment.getCamille();
            String content = c.get(PathBuilder.buildSchedulingGroupPath(CamilleEnvironment.getPodId())).getData();
            log.debug("Retrieving scheduling group for scheduler {}", schedulerName);
            Map<String, String> jsonMap = JsonUtils.convertMap(om.readValue(content, HashMap.class), String.class,
                    String.class);
            return filterDetail(schedulerName, jsonMap);
        } catch (Exception e) {
            log.error("Failed to retrieve scheduling group for scheduler {}", schedulerName, e);
            throw new RuntimeException(e);
        }
    }

    /*-
     * list of tenants that will NOT be considered as large tenants.
     * this is added to make important tenants' queue time faster
     */
    private Set<String> getLargeTenantExemptionSet() {
        try {
            Camille c = CamilleEnvironment.getCamille();
            String tenantsStr = c
                    .get(PathBuilder.buildSchedulingLargeTenantExemptionListPath(CamilleEnvironment.getPodId()))
                    .getData();
            if (StringUtils.isEmpty(tenantsStr)) {
                return Collections.emptySet();
            }
            String[] tenants = tenantsStr.split(",");
            log.debug("Retrieving large tenant exemption list. tenants = {}", tenantsStr);
            return Arrays.stream(tenants) //
                    .filter(StringUtils::isNotBlank) //
                    .map(CustomerSpace::shortenCustomerSpace) //
                    .filter(StringUtils::isNotBlank) //
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Failed to retrieve large tenant exemption list", e);
            return Collections.emptySet();
        }
    }

    private void setSchedulerQuotaLimit() {
        try {
            Camille c = CamilleEnvironment.getCamille();
            Path configPath = PathBuilder.buildSchedulerConfigPath(CamilleEnvironment.getPodId());
            if (c.exists(configPath)) {
                String configStr = c.get(configPath).getData();
                if (StringUtils.isEmpty(configStr)) {
                    return;
                }
                PASchedulerConfig quotaLimit = JsonUtils.deserialize(configStr, PASchedulerConfig.class);
                if (quotaLimit.getLargeTenantAccountVolumeThreshold() != null) {
                    largeAccountCountLimit = quotaLimit.getLargeTenantAccountVolumeThreshold();
                }
                if (quotaLimit.getConcurrentLargeJobLimit() != null) {
                    maxLargeJobCount = quotaLimit.getConcurrentLargeJobLimit();
                }
                if (quotaLimit.getLargeTenantTxnVolumeThreshold() != null) {
                    largeTransactionCountLimit = quotaLimit.getLargeTenantTxnVolumeThreshold();
                }
                if (quotaLimit.getRetryCountLimit() != null) {
                    processAnalyzeJobRetryCount = quotaLimit.getRetryCountLimit();
                }
                if (quotaLimit.getRetryExpiredTime() != null) {
                    retryExpiredTime = quotaLimit.getRetryExpiredTime();
                }
                if (quotaLimit.getConcurrentScheduleNowJobLimit() != null) {
                    maxScheduleNowJobCount = quotaLimit.getConcurrentScheduleNowJobLimit();
                }
                if (quotaLimit.getConcurrentJobLimit() != null) {
                    concurrentProcessAnalyzeJobs = quotaLimit.getConcurrentJobLimit();
                }
                log.info("Retrieving SchedulerConfig. config = {}", configStr);
            }
        } catch (Exception e) {
            log.warn("Failed to retrieve SchedulerConfig", e);
        }

        log.info(
                "large tenant threshold (account={}, txn={}). concurrency limit (total={}, scheduleNow={}, large={}, largeTxn={}). Retry limit={}, Retry expired time={}",
                largeAccountCountLimit, largeTransactionCountLimit, concurrentProcessAnalyzeJobs,
                maxScheduleNowJobCount, maxLargeJobCount, maxLargeTxnJobCount, processAnalyzeJobRetryCount,
                retryExpiredTime);
    }

    private static String filterDetail(String stackName, Map<String, String> nodes) {
        String filterName = stackName + SCHEDULING_GROUP_SUFFIX;
        return nodes.getOrDefault(filterName, DEFAULT_SCHEDULING_GROUP);
    }

    private static String filterDetailForSchedulingPAFlag(String schedulerName, Map<String, String> nodes) {
        String filterName = schedulerName + SCHEDULING_GROUP_SUFFIX;
        return nodes.getOrDefault(filterName, "");
    }

    @Override
    public boolean isSchedulerEnabled(@NotNull String schedulerName) {
        try {
            Camille c = CamilleEnvironment.getCamille();
            String content = c.get(PathBuilder.buildSchedulingPAFlagPath(CamilleEnvironment.getPodId())).getData();
            Map<String, String> jsonMap = JsonUtils.convertMap(om.readValue(content, HashMap.class), String.class,
                    String.class);
            log.debug("Checking whether scheduler [{}] is enabled. SchedulingFlags={}", schedulerName, jsonMap);
            return "On".equalsIgnoreCase(filterDetailForSchedulingPAFlag(schedulerName, jsonMap));
        } catch (Exception e) {
            log.error("Failed to check whether scheduler [{}] is enabled", schedulerName);
            return false;
        }
    }

    private Map<String, Long> getPASubmitFailedRedisMap() {
        String redisKey = "pa_scheduler_" + leEnv + "_pa_submit_failed";
        Map<String, Long> paSubmitFailedMap = new HashMap<>();
        try {
            Map<Object, Object> redisMap = redisTemplate.opsForHash().entries(redisKey);
            if (redisMap.size() > 0) {
                for (Map.Entry<Object, Object> entry : redisMap.entrySet()) {
                    Long failedTime = (Long) entry.getValue();
                    if (schedulingPATimeClock.getCurrentTime() - failedTime < 60 * 60 * 1000) {
                        paSubmitFailedMap.put((String) entry.getKey(), failedTime);
                    } else {
                        redisTemplate.opsForHash().delete(redisKey, entry.getKey());
                    }
                }

            }
        } catch (Exception e) {
            log.error("get redis cache fail.", e);
        }
        return paSubmitFailedMap;
    }

    private Set<ActionType> getNonIngestAndReplaceActionType() {
        Set<ActionType> nonIngestAndReplaceActionType = new HashSet<>(Arrays.asList(ActionType.values()));
        nonIngestAndReplaceActionType.remove(ActionType.DATA_REPLACE);
        nonIngestAndReplaceActionType.remove(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        nonIngestAndReplaceActionType.remove(ActionType.CDL_OPERATION_WORKFLOW);
        return nonIngestAndReplaceActionType;
    }

    @VisibleForTesting
    void setTimeClock(TimeClock clock) {
        this.schedulingPATimeClock = clock;
    }

    public boolean reachFailCountLimit(String tenantId, int maxFailCount) {
        try {
            String failCountKey = CacheName.Constants.PAFailCountCacheName + "_" + tenantId;
            Integer currentCount = (Integer) redisTemplate.opsForValue().get(failCountKey);
            if (currentCount == null) {
                log.debug("tenantId = {} and failcount is 0", tenantId);
                return false;
            }
            log.debug("tenantId = {} and failcount is {}", tenantId, currentCount);
            return currentCount >= maxFailCount;
        } catch (Exception e) {
            log.error("get redis cache fail for tenant " + tenantId, e);
        }
        return false;
    }


    private boolean isNewActionAdded(String tenantId, Date lastActionTimeInDB) {
        try {
            String lastActionTimeKey = CacheName.LastActionTimeCache.getKeyForCache(tenantId);
            Long lastActionTimeInCache = (Long) redisTemplate.opsForValue().get(lastActionTimeKey);
            if(lastActionTimeInCache == null || !lastActionTimeInCache.equals(lastActionTimeInDB.getTime())){
                log.debug("New Action added for {}", tenantId);
                return true;
            } else {
                log.debug("No new Action added for {}", tenantId);
                return false;
            }
        } catch (Exception e) {
            log.error("get redis cache fail for tenant " + tenantId, e);
            return false;
        }
    }

}
