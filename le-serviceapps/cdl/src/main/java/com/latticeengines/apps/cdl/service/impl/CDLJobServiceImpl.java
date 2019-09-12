package com.latticeengines.apps.cdl.service.impl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.cdl.workflow.EntityExportWorkflowSubmitter;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingResult;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.pls.PlsHealthCheckProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

import javafx.util.Pair;

@Component("cdlJobService")
public class CDLJobServiceImpl implements CDLJobService {

    private static final Logger log = LoggerFactory.getLogger(CDLJobServiceImpl.class);

    private static final String USERID = "Auto Scheduled";
    private static final String STACK_INFO_URL = "/pls/health/stackinfo";

    private static final String TEST_SCHEDULER = "qa_testing";
    private static final String PA_JOB_TYPE = "processAnalyzeWorkflow";
    private static final String USER_ERROR_CATEGORY = "User Error";

    @Value("${common.le.environment}")
    private String leEnv;

    private static String MAIN_TRACKING_SET;

    private static String NEW_TRACKING_SET;

    private static String REDIS_TEMPLATE_KEY;

    @Inject
    private EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;

    @VisibleForTesting
    static LinkedHashMap<String, Long> appIdMap;

    private static LinkedHashMap<String, Pair> EXPORT_APPID_MAP;

    @Inject
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private AtlasSchedulingService atlasSchedulingService;

    @Inject
    private SchedulingPAService schedulingPAService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private AtlasExportService atlasExportService;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.concurrent.job.count}")
    int concurrentProcessAnalyzeJobs;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.minimum.scheduled.job.count}")
    int minimumScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.maximum.scheduled.job.count}")
    int maximumScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.job.retry.count:1}")
    private int processAnalyzeJobRetryCount;

    @VisibleForTesting
    @Value("${common.adminconsole.url:}")
    private String quartzMicroserviceHostPort;

    @VisibleForTesting
    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @VisibleForTesting
    @Value("${common.quartz.stack.flag:false}")
    private boolean isQuartzStack;

    @Value("${common.le.stack}")
    private String leStack;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    private CDLProxy cdlProxy;

    @Inject
    private PlsHealthCheckProxy plsHealthCheckProxy;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @PostConstruct
    public void init() {
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        appIdMap = new LinkedHashMap<String, Long>() {
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return System.currentTimeMillis() - eldest.getValue() > TimeUnit.HOURS.toMillis(2);
            }
        };
        EXPORT_APPID_MAP = new LinkedHashMap<>();

        initTrackingSets();
    }

    private List<String> types = Collections.singletonList("processAnalyzeWorkflow");
    private List<String> exportTypes = Collections.singletonList("entityExportWorkflow");
    private List<String> jobStatuses = Collections.singletonList(JobStatus.RUNNING.getName());

    @PostConstruct
    public void initialize() {
        if (isQuartzStack) {
            cdlProxy = new CDLProxy(quartzMicroserviceHostPort);
            log.info(String.format("CDLJobService running on quartz stack with cdlHostPort=%s, workflowHostPort=%s",
                    cdlProxy.getHostport(), workflowProxy.getHostport()));
        } else {
            cdlProxy = new CDLProxy(microserviceHostPort);
            log.info(String.format("CDLJobService running with cdlHostPort=%s, workflowHostPort=%s",
                    cdlProxy.getHostport(), workflowProxy.getHostport()));
        }

        initTrackingSets();
    }

    @Override
    public boolean submitJob(CDLJobType cdlJobType, String jobArguments) {
        boolean isActivityBasedPA = schedulingPAService.isSchedulerEnabled(leStack);
        if (cdlJobType == CDLJobType.PROCESSANALYZE) {
            if (!isActivityBasedPA) {
                checkAndUpdateJobStatus(CDLJobType.PROCESSANALYZE);
            }
            try {
                if (!systemCheck() && !isActivityBasedPA) {
                    orchestrateJob();
                }
            } catch (Exception e) {
                log.error("orchestrateJob CDLJobType.PROCESSANALYZE failed: ", e);
                throw e;
            }
        } else if (cdlJobType == CDLJobType.EXPORT) {
            try {
                exportScheduleJob();
            } catch (Exception e) {
                log.error("schedule CDLJobType.EXPORT failed" + e.getMessage());
                throw e;
            }
        } else if (cdlJobType == CDLJobType.SCHEDULINGPA) {
            try {
                // always run the stack scheduler, use the flag to determine whether it is in
                // dryRun mode
                schedulePAJob(leStack, !isActivityBasedPA);

                // test scheduler for QA, only evaluate system status if the flag is set
                if (schedulingPAService.isSchedulerEnabled(TEST_SCHEDULER)) {
                    schedulePAJob(TEST_SCHEDULER, false);
                }
            } catch (Exception e) {
                log.error("schedule CDLJobType.SCHEDULINGPA failed", e);
                throw e;
            }
        }
        return true;
    }

    @VisibleForTesting
    boolean systemCheck() {
        StatusDocument statusDocument = plsHealthCheckProxy.systemCheck();
        log.info(String.format("System status is : %s.", statusDocument.getStatus()));
        return StatusDocument.UNDER_MAINTENANCE.equals(statusDocument.getStatus());
    }

    @Override
    public Date getNextInvokeTime(CustomerSpace customerSpace) {
        Tenant tenantInContext = MultiTenantContext.getTenant();
        try {
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
            MultiTenantContext.setTenant(tenant);
            CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr
                    .findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
            return getNextInvokeTime(customerSpace, tenant, processAnalyzeJobDetail);
        } finally {
            MultiTenantContext.setTenant(tenantInContext);
        }
    }

    @VisibleForTesting
    Date getNextInvokeTime(CustomerSpace customerSpace, Tenant tenant, CDLJobDetail processAnalyzeJobDetail) {
        Date invokeTime = null;

        boolean allowAutoSchedule = false;
        try {
            allowAutoSchedule = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        } catch (Exception e) {
            log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
        }
        if (allowAutoSchedule) {
            int invokeHour = zkConfigService.getInvokeTime(customerSpace, CDLComponent.componentName);
            Tenant tenantInContext = MultiTenantContext.getTenant();
            try {
                MultiTenantContext.setTenant(tenant);
                invokeTime = getInvokeTime(processAnalyzeJobDetail, invokeHour, new Date(tenant.getRegisteredTime()));
            } finally {
                MultiTenantContext.setTenant(tenantInContext);
            }
        }
        return invokeTime;
    }

    private void orchestrateJob() {
        String clusterId = getCurrentClusterID();
        log.debug(String.format("Current cluster id is : %s.", clusterId));
        boolean clusterIdIsEmpty = StringUtils.isEmpty(clusterId);

        int runningPAJobsCount = 0;
        int autoScheduledPAJobsCount = 0;
        long currentTimeMillis = System.currentTimeMillis();

        List<SimpleDataFeed> allDataFeeds = dataFeedService.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0");
        log.info(String.format("DataFeed for active tenant count: %d.", allDataFeeds.size()));

        List<SimpleDataFeed> processAnalyzingDataFeeds = new ArrayList<>();
        List<Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>>> list = new ArrayList<>();
        for (SimpleDataFeed dataFeed : allDataFeeds) {
            Tenant tenant = dataFeed.getTenant();
            if (clusterIdIsEmpty && dataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                runningPAJobsCount++;
                processAnalyzingDataFeeds.add(dataFeed);

                if (isAutoScheduledPAJob(tenant.getId())) {
                    autoScheduledPAJobsCount++;
                }
            } else if (dataFeed.getStatus() == DataFeed.Status.Active) {
                MultiTenantContext.setTenant(tenant);
                CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
                Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, cdlJobDetail);
                if (invokeTime != null) {
                    if (dataFeed.getNextInvokeTime() == null || !dataFeed.getNextInvokeTime().equals(invokeTime)) {
                        dataFeedService.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                    }
                    if (currentTimeMillis > invokeTime.getTime()) {
                        list.add(new HashMap.SimpleEntry<>(invokeTime,
                                new HashMap.SimpleEntry<>(dataFeed, cdlJobDetail)));
                    }
                }
            }
        }
        log.info(String.format("Need to run PA job count: %d.", list.size()));

        StringBuilder sb = new StringBuilder();
        if (clusterIdIsEmpty) {
            sb.append(String.format("Have %d running PA jobs. ", runningPAJobsCount));
            for (SimpleDataFeed dataFeed : processAnalyzingDataFeeds) {
                sb.append(String.format("Tenant %s is running PA job. ", dataFeed.getTenant().getId()));
            }
        } else {
            List<WorkflowJob> runningPAJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(clusterId, types,
                    jobStatuses);
            for (WorkflowJob job : runningPAJobs) {
                appIdMap.remove(job.getApplicationId());
            }
            runningPAJobsCount = runningPAJobs.size();

            for (WorkflowJob workflowJob : runningPAJobs) {
                if (USERID.equals(workflowJob.getUserId())) {
                    autoScheduledPAJobsCount++;
                }
            }
            if (appIdMap.size() > 0) {
                log.info(String.format("There's %d applications submitted but cannot get from job query: %s",
                        appIdMap.size(), appIdMap.entrySet().toString()));
                autoScheduledPAJobsCount += appIdMap.size();
            }

            sb.append(String.format("Have %d running PA jobs. ", runningPAJobsCount));
            for (WorkflowJob workflowJob : runningPAJobs) {
                sb.append(String.format("Tenant %s run by %s. ", workflowJob.getTenant().getId(),
                        workflowJob.getUserId()));
            }
        }
        log.info(sb.toString());

        if ((runningPAJobsCount < concurrentProcessAnalyzeJobs && autoScheduledPAJobsCount < maximumScheduledJobCount)
                || runningPAJobsCount >= concurrentProcessAnalyzeJobs
                && autoScheduledPAJobsCount < minimumScheduledJobCount) {
            log.info("Need to schedule a PA job.");
            list.sort(Comparator.comparing(Map.Entry::getKey));
            for (Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>> entry : list) {
                SimpleDataFeed dataFeed = entry.getValue().getKey();
                CDLJobDetail cdlJobDetail = entry.getValue().getValue();
                if (submitProcessAnalyzeJob(dataFeed.getTenant(), cdlJobDetail)) {
                    log.info(String.format("Submitted invoke time: %s, tenant name: %s.", entry.getKey(),
                            dataFeed.getTenant().getName()));
                    break;
                }
            }
        } else {
            log.info("No need to schedule a PA job.");
        }
    }

    @Override
    public void schedulePAJob(@NotNull String schedulerName, boolean dryRun) {
        if (systemCheck()) {
            return;
        }

        SchedulingResult result = schedulingPAService.getSchedulingResult(schedulerName);
        log.info(
                "Scheduled new PAs for tenants = {}, retry PAs for tenants = {}. schedulerName={}, dryRun={}, totalSize={}",
                result.getNewPATenants(), result.getRetryPATenants(), schedulerName, dryRun,
                result.getNewPATenants().size() + result.getRetryPATenants().size());

        try {
            Set<?> starvedTenants = detectStarvation(result);
            if (CollectionUtils.isNotEmpty(starvedTenants)) {
                log.warn("Starvation detected for the following tenant(s) in scheduler: {}", starvedTenants);
            }
        } catch (Exception e) {
            log.error("Encounter error while detecting starvation:", e);
        }

        Set<String> canRunRetryJobSet = result.getRetryPATenants();
        if (CollectionUtils.isNotEmpty(canRunRetryJobSet)) {
            for (String tenantId : canRunRetryJobSet) {
                try {
                    if (!dryRun && retryValidation(tenantId)) {
                        ApplicationId retryAppId = cdlProxy.restartProcessAnalyze(tenantId, Boolean.TRUE);
                        logScheduledPA(schedulerName, tenantId, retryAppId, true, result);
                        if (retryAppId != null) {
                            removeFromSet(retryAppId, MAIN_TRACKING_SET, tenantId);
                        }
                    } else {
                        removeFromSet(null, MAIN_TRACKING_SET, tenantId);
                    }
                } catch (Exception e) {
                    log.error("Failed to retry PA for tenant {}, error = {}", tenantId, e);
                    updateRetryCount(tenantId);
                    setPASubmissionFailTime(REDIS_TEMPLATE_KEY, tenantId);
                }
            }
        }
        Set<String> canRunJobSet = result.getNewPATenants();
        if (CollectionUtils.isNotEmpty(canRunJobSet)) {
            for (String tenantId : canRunJobSet) {
                if (!dryRun) {
                    ApplicationId appId = submitProcessAnalyzeJob(tenantId);
                    logScheduledPA(schedulerName, tenantId, appId, false, result);
                    if (appId != null) {
                        removeFromSet(appId, MAIN_TRACKING_SET, tenantId);
                    }
                } else {
                    removeFromSet(null, MAIN_TRACKING_SET, tenantId);
                }
            }
        }
    }

    private void logScheduledPA(@NotNull String schedulerName, String tenantId, ApplicationId appId, boolean isRetry,
                                SchedulingResult result) {
        if (StringUtils.isEmpty(tenantId) || appId == null || appId.toString() == null || result == null) {
            return;
        }

        SchedulingResult.Detail detail = result.getDetails().get(tenantId);
        log.info("Scheduled PA for tenant='{}', applicationId='{}', isRetry='{}', detail='{}', schedulerName='{}'",
                tenantId, appId.toString(), isRetry, JsonUtils.serialize(detail), schedulerName);
    }

    private void updateRetryCount(String tenantId) {
        try {
            // TODO test this call, throw error once
            Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
            MultiTenantContext.setTenant(tenant);
            dataFeedService.increasedRetryCount(MultiTenantContext.getShortTenantId());
        } catch (Exception e) {
            log.error("Failed to increase retry count for tenant {}. error = {}", tenantId, e);
        }
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    String getCurrentClusterID() {
        String clusterId = null;
        try {
            AtomicReference<Map<String, String>> stackInfo = new AtomicReference<>();
            stackInfo.set(plsHealthCheckProxy.getActiveStack());
            if (MapUtils.isNotEmpty(stackInfo.get()) && stackInfo.get().containsKey("EMRClusterId")) {
                clusterId = stackInfo.get().get("EMRClusterId");
            }
        } catch (Exception e) {
            log.error("Get current cluster id failed. ", e);
        }
        return clusterId;
    }

    private Date getInvokeTime(CDLJobDetail processAnalyzeJobDetail, int invokeHour, Date tenantCreateDate) {
        Calendar calendar = Calendar.getInstance();
        if (processAnalyzeJobDetail == null) {
            calendar.setTime(new Date(System.currentTimeMillis()));

            calendar.set(Calendar.HOUR_OF_DAY, invokeHour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            if (calendar.getTime().before(tenantCreateDate)) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
            }
            return calendar.getTime();
        } else if (processAnalyzeJobDetail.getCdlJobStatus() == CDLJobStatus.RUNNING) {
            return null;
        } else {
            if ((processAnalyzeJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL)
                    && (processAnalyzeJobDetail.getRetryCount() < processAnalyzeJobRetryCount)) {
                calendar.setTime(processAnalyzeJobDetail.getCreateDate());
            } else {
                calendar.setTime(processAnalyzeJobDetail.getCreateDate());
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

    private int checkAndUpdateJobStatus(CDLJobType cdlJobType) {
        List<CDLJobDetail> details = cdlJobDetailEntityMgr.listAllRunningJobByJobType(cdlJobType);
        int runningJobs = details.size();
        for (CDLJobDetail cdlJobDetail : details) {
            String appId = cdlJobDetail.getApplicationId();
            if (StringUtils.isNotEmpty(appId)) {
                Job job = workflowProxy.getWorkflowJobFromApplicationId(appId);
                if (job != null && !job.isRunning()) {
                    updateOneJobStatus(cdlJobType, cdlJobDetail, job);
                    runningJobs--;
                }
            }
        }
        return runningJobs;
    }

    @VisibleForTesting
    boolean submitProcessAnalyzeJob(Tenant tenant, CDLJobDetail cdlJobDetail) {
        MultiTenantContext.setTenant(tenant);

        ApplicationId applicationId = null;
        int retryCount;
        boolean success = true;

        if ((cdlJobDetail == null) || (cdlJobDetail.getCdlJobStatus() != CDLJobStatus.FAIL)) {
            retryCount = 0;
        } else {
            retryCount = cdlJobDetail.getRetryCount() + 1;
        }
        boolean retry = retryProcessAnalyze(tenant, cdlJobDetail);
        cdlJobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE, tenant);
        try {
            if (retry) {
                applicationId = cdlProxy.restartProcessAnalyze(tenant.getId(), Boolean.TRUE);
            } else {
                ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
                request.setUserId(USERID);
                request.setAutoSchedule(true);
                applicationId = cdlProxy.processAnalyze(tenant.getId(), request);
            }
            appIdMap.put(applicationId.toString(), System.currentTimeMillis());
            cdlJobDetail.setApplicationId(applicationId.toString());
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
        } catch (Exception e) {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            log.info(String.format("Failed to submit job for tenant name: %s", tenant.getName()));
            success = false;
        }
        cdlJobDetail.setRetryCount(retryCount);
        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        log.info(String.format(
                "Submit process analyze job with application id: %s, tenant id: %s, retry: %s, success" + " %s",
                applicationId, tenant.getName(), retry ? "y" : "n", success ? "y" : "n"));
        return true;
    }

    private ApplicationId submitProcessAnalyzeJob(String tenantId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        MultiTenantContext.setTenant(tenant);
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(MultiTenantContext.getShortTenantId());
        ApplicationId applicationId = null;
        try {
            ProcessAnalyzeRequest request;
            if (StringUtils.isNotEmpty(dataFeed.getScheduleRequest())) {
                request = JsonUtils.deserialize(dataFeed.getScheduleRequest(), ProcessAnalyzeRequest.class);
            } else {
                request = new ProcessAnalyzeRequest();
                request.setUserId(USERID);
            }
            applicationId = cdlProxy.scheduleProcessAnalyze(tenant.getId(), true, request);
            log.info("Submit PA job with appId = {} for tenant = {} successfully", applicationId, tenantId);
        } catch (Exception e) {
            log.error("Failed to submit job for tenant = {}, error = {}", tenantId, e);
            setPASubmissionFailTime(REDIS_TEMPLATE_KEY, tenantId);
        }
        return applicationId;
    }

    @VisibleForTesting
    boolean retryProcessAnalyze(Tenant tenant, CDLJobDetail cdlJobDetail) {
        DataFeedExecution execution;
        try {
            DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(tenant.getId());
            execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(dataFeed,
                    DataFeedExecutionJobType.PA);
        } catch (Exception e) {
            execution = null;
        }
        if ((execution != null) && (DataFeedExecution.Status.Failed.equals(execution.getStatus()))) {
            if ((cdlJobDetail == null) || !reachRetryLimit(CDLJobType.PROCESSANALYZE, cdlJobDetail.getRetryCount())) {
                return true;
            } else {
                log.info(String.format("Tenant %s exceeds retry limit and skip failed exeuction", tenant.getName()));
            }
        }
        return false;
    }

    private void updateOneJobStatus(CDLJobType cdlJobType, CDLJobDetail cdlJobDetail, Job job) {
        JobStatus jobStatus = job.getJobStatus();
        String customerSpace = cdlJobDetail.getTenant().getId();
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (jobStatus == JobStatus.COMPLETED) {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.COMPLETE);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
            if (reachRetryLimit(cdlJobType, cdlJobDetail.getRetryCount())
                    && dataFeed.getDrainingStatus() != DrainingStatus.NONE) {
                dataFeedService.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(),
                        DrainingStatus.NONE.name());
            }
        }
    }

    private boolean reachRetryLimit(CDLJobType cdlJobType, int retryCount) {
        switch (cdlJobType) {
            case PROCESSANALYZE:
                return retryCount >= processAnalyzeJobRetryCount;
            default:
                return false;
        }
    }

    private boolean isAutoScheduledPAJob(String tenantId) {
        List<Job> jobs = workflowProxy.getJobs(null, types, jobStatuses, false, tenantId);
        if (jobs != null) {
            if (jobs.size() == 1) {
                return USERID.equals(jobs.get(0).getUser());
            } else {
                log.warn(String.format("There are more than one running PA jobs for tenant %s", tenantId));
            }
        }
        return false;
    }

    private void exportScheduleJob() {
        updateExportAppIdMap();
        List<AtlasScheduling> atlasSchedulingList = atlasSchedulingService
                .findAllByType(AtlasScheduling.ScheduleType.Export);
        if (CollectionUtils.isNotEmpty(atlasSchedulingList)) {
            log.info(String.format("Need export entity tenant count: %d.", atlasSchedulingList.size()));
            for (AtlasScheduling atlasScheduling : atlasSchedulingList) {
                Tenant tenant = atlasScheduling.getTenant();
                String customerSpace = CustomerSpace.shortenCustomerSpace(tenant.getId());
                boolean allowAutoSchedule = false;
                try {
                    allowAutoSchedule = batonService.isEnabled(CustomerSpace.parse(tenant.getId()),
                            LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
                } catch (Exception e) {
                    log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
                }
                if (allowAutoSchedule) {
                    Long nextFireTime = CronUtils.getNextFireTime(atlasScheduling.getCronExpression()).getMillis()
                            / 1000;
                    boolean triggered = false;
                    boolean changed = false;
                    if (atlasScheduling.getNextFireTime() == null) {
                        atlasScheduling.setNextFireTime(nextFireTime);
                        changed = true;
                    } else {
                        Long currentSecond = (new Date().getTime()) / 1000;
                        if (atlasScheduling.getNextFireTime() <= currentSecond) {
                            atlasScheduling.setPrevFireTime(atlasScheduling.getNextFireTime());
                            atlasScheduling.setNextFireTime(nextFireTime);
                            triggered = true;
                            changed = true;
                        } else {
                            if (nextFireTime - atlasScheduling.getNextFireTime() != 0) {
                                atlasScheduling.setNextFireTime(nextFireTime);
                                changed = true;
                            }
                        }
                    }
                    if (changed) {
                        atlasSchedulingService.updateExportScheduling(atlasScheduling);
                    }
                    if (triggered) {
                        List<AtlasExportType> atlasExportTypes = Arrays.asList(AtlasExportType.ACCOUNT, AtlasExportType.CONTACT);
                        for (AtlasExportType atlasExportType : atlasExportTypes) {
                            Pair<String, AtlasExportType> pair = new Pair<>(customerSpace, atlasExportType);
                            if (EXPORT_APPID_MAP.containsValue(pair)) {
                                continue;
                            }
                            if (submitExportJob(customerSpace, tenant, atlasExportType)) {
                                log.info(String.format("ExportJob submitted invoke time: %s, tenant name: %s, export type: %s.",
                                        atlasScheduling.getPrevFireTime(), tenant.getName(), atlasExportType));
                            }
                        }
                    }
                }
            }
        }
    }

    boolean submitExportJob(String customerSpace, Tenant tenant, AtlasExportType atlasExportType) {
        return submitExportJob(customerSpace, false, tenant, atlasExportType);
    }

    @VisibleForTesting
    boolean submitExportJob(String customerSpace, boolean retry, Tenant tenant, AtlasExportType atlasExportType) {
        String applicationId;
        boolean success = true;
        if (tenant == null) {
            setMultiTenantContext(customerSpace);
        } else {
            MultiTenantContext.setTenant(tenant);
        }
        try {
            AtlasExport atlasExport = atlasExportService.createAtlasExport(customerSpace, atlasExportType);
            EntityExportRequest request = new EntityExportRequest();
            request.setAtlasExportId(atlasExport.getUuid());
            request.setSaveToDropfolder(true);
            request.setDataCollectionVersion(dataCollectionProxy.getActiveVersion(customerSpace));
            ApplicationId tempApplicationId = entityExportWorkflowSubmitter.submit(customerSpace, request, new WorkflowPidWrapper(-1L));
            applicationId = tempApplicationId.toString();
            if (!retry) {
                Pair<String, AtlasExportType> value = new Pair<>(customerSpace, atlasExportType);
                EXPORT_APPID_MAP.put(applicationId, value);
            }
            log.info("export applicationId map is " + JsonUtils.serialize(EXPORT_APPID_MAP));
        } catch (Exception e) {
            log.info(String.format("Failed to submit entity export job for tenant name: %s，message is %s",
                    customerSpace, e.getMessage()));
            success = false;
        }
        log.info(String.format("Submit entity export job success %s", success ? "y" : "n"));
        return success;
    }

    private void updateExportAppIdMap() {
        String clusterId = getCurrentClusterID();
        log.debug(String.format("Current cluster id is : %s.", clusterId));
        List<String> jobStatus = new ArrayList<>();
        jobStatus.add(JobStatus.FAILED.getName());
        jobStatus.add(JobStatus.CANCELLED.getName());
        jobStatus.add(JobStatus.COMPLETED.getName());
        if (StringUtils.isNotEmpty(clusterId)) {
            List<WorkflowJob> workflowJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(clusterId, exportTypes,
                    jobStatus);
            if (CollectionUtils.isNotEmpty(workflowJobs)) {
                for (WorkflowJob workflowJob : workflowJobs) {
                    if (workflowJob.getStatus() == JobStatus.COMPLETED.getName()
                            || workflowJob.getStatus() == JobStatus.CANCELLED.getName()) {
                        EXPORT_APPID_MAP.remove(workflowJob.getApplicationId());
                        continue;
                    }
                    if (workflowJob.getStatus() == JobStatus.FAILED.getName()) {
                        Pair<String, AtlasExportType> pair = EXPORT_APPID_MAP.get(workflowJob.getApplicationId());
                        if (pair != null) {
                            submitExportJob(pair.getKey(), true, null, pair.getValue());
                            EXPORT_APPID_MAP.remove(workflowJob.getApplicationId());
                        }
                    }
                }
            }
        }
    }

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }

    @VisibleForTesting
    Set<?> detectStarvation(SchedulingResult result) {
        Set<String> allTenantsInQ = result.getAllTenantsInQ();
        Set<ZSetOperations.TypedTuple<Object>> newTrackingEntries = new HashSet<>();
        Instant curTime = Instant.now();
        allTenantsInQ.forEach(tenantId -> {
            newTrackingEntries.add(new DefaultTypedTuple<>(tenantId, (double) curTime.toEpochMilli()));
        });
        if (CollectionUtils.isNotEmpty(newTrackingEntries)) {
            redisTemplate.opsForZSet().add(NEW_TRACKING_SET, newTrackingEntries);
            redisTemplate.opsForZSet().unionAndStore(MAIN_TRACKING_SET, Collections.singletonList(NEW_TRACKING_SET), MAIN_TRACKING_SET, RedisZSetCommands.Aggregate.MIN);
            redisTemplate.delete(NEW_TRACKING_SET);
        }
        Set<?> starvedTenants = redisTemplate.opsForZSet().rangeByScore(MAIN_TRACKING_SET, 0, (double) curTime.minus(2, ChronoUnit.DAYS).toEpochMilli());
        if (CollectionUtils.isNotEmpty(starvedTenants)) {
            return starvedTenants;
        }
        return Collections.emptySet();
    }

    private void removeFromSet(ApplicationId appId, String key, String tenantId) {
        log.info("Submitted PA for tenant {}. Application is {}. Removing from tracking set {}", tenantId, appId, key);
        try {
            redisTemplate.opsForZSet().remove(key, tenantId);
        } catch (Exception e) {
            log.warn("Failed to remove tenant {} from tracking set {}. {}", tenantId, key, e);
        }
    }

    private void initTrackingSets() {
        MAIN_TRACKING_SET = leEnv + "_PA_SCHEDULER_MAIN_TRACKING_SET";
        NEW_TRACKING_SET = leEnv + "_PA_SCHEDULER_NEW_TRACKING_SET";
        REDIS_TEMPLATE_KEY = "pa_scheduler_" + leEnv + "_pa_submit_failed";
    }

    private void setPASubmissionFailTime(String key, String customerSpace) {
        try {
            long currentTime = new Date().getTime();
            redisTemplate.opsForHash().put(key, customerSpace, currentTime);
            log.info("pa submit failed, tenant: " + customerSpace + ", fail time: " + currentTime);
        } catch (Exception e) {
            log.error("get redis cache fail.", e);
        }
    }

    /**
     * Don't retry if last PA failed due to user error
     * will update retryCount in table DataFeedExecution
     */
    private boolean retryValidation(String tenantId) {
        try {
            DataFeedExecution execution = getLastFailedPAExecution(tenantId);
            if (execution.getWorkflowId() == null) {
                execution.setRetryCount(execution.getRetryCount() + 1);
                dataFeedExecutionEntityMgr.updateRetryCount(execution);
                log.warn("cannot find workflowId, tenant {} cannot be retry.", tenantId);
                return false;
            }
            Job job = getFailedPAJob(execution, tenantId);
            if (USER_ERROR_CATEGORY.equalsIgnoreCase(job.getErrorCategory())) {
                execution.setRetryCount(execution.getRetryCount() + 1);
                dataFeedExecutionEntityMgr.updateRetryCount(execution);
                log.warn("due to user error, tenant {} cannot be retry.", tenantId);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("cannot retry this tenant {}, error: {}", tenantId, e);
            return false;
        }
    }

    private DataFeedExecution getLastFailedPAExecution(String tenantId) {
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(tenantId);
        if (dataFeed == null) {
            throw new NullPointerException("can not find datafeed in tenant " + tenantId);
        }
        DataFeedExecution execution;
        execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(dataFeed,
                DataFeedExecutionJobType.PA);
        if (execution.getWorkflowId() == null) {
            throw new NullPointerException("can not find workflowId in tenant " + tenantId);
        }
        return execution;
    }

    private Job getFailedPAJob(DataFeedExecution execution, String tenantId) {

        Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()), tenantId);
        if (job == null || !PA_JOB_TYPE.equalsIgnoreCase(job.getJobType()) || job.getJobStatus() != JobStatus.FAILED) {
            throw new IllegalArgumentException("the last pa job isn't failed in tenant " + tenantId);
        }
        return job;
    }
}
