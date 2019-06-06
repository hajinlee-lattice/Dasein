package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.util.PriorityQueueUtils;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ActivityObject;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
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
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("cdlJobService")
public class CDLJobServiceImpl implements CDLJobService {

    private static final Logger log = LoggerFactory.getLogger(CDLJobServiceImpl.class);

    private static final String LE_STACK = "LE_STACK";
    private static final String QUARTZ_STACK = "quartz";
    private static final String USERID = "Auto Scheduled";
    private static final String STACK_INFO_URL = "/pls/health/stackinfo";

    @VisibleForTesting
    static String CACHE_KEY_PREFIX = CacheName.Constants.CDLScheduledJobCacheName;

    private static final String DELIMITER = ":";
    @VisibleForTesting
    static String HIGH_PRIORITY_RUNNING_JOB_MAP = CACHE_KEY_PREFIX + DELIMITER +
        "HIGH_PRIORITY_RUNNING_JOB_MAP";

    @VisibleForTesting
    static String LOW_PRIORITY_RUNNING_JOB_MAP = CACHE_KEY_PREFIX + DELIMITER +
            "LOW_PRIORITY_RUNNING_JOB_MAP";

    @VisibleForTesting
    static LinkedHashMap<String, Long> appIdMap;

    static LinkedHashMap<String, String> EXPORT_APPID_MAP;

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
    private ActionService actionService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

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

    @Inject
    private WorkflowProxy workflowProxy;

    private CDLProxy cdlProxy;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.maximum.high.priority.scheduled.job.count}")
    int maximumHighPriorityScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.minimum.high.priority.scheduled.job.count}")
    int minimumHighPriorityScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.maximum.low.priority.scheduled.job.count}")
    int maximumLowPriorityScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.processAnalyze.minimum.low.priority.scheduled.job.count}")
    int minimumLowPriorityScheduledJobCount;

    @VisibleForTesting
    @Value("${cdl.activity.based.pa}")
    boolean isActivityBasedPA;

    @Value("${cdl.app.public.url:https://localhost:9081}")
    private String appPublicUrl;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    private Map<String, JobProperty> highMap = null;
    private Map<String, JobProperty> lowMap = null;

    @PostConstruct
    public void init() {
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        appIdMap = new LinkedHashMap<String, Long>() {
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return System.currentTimeMillis() - eldest.getValue() > TimeUnit.HOURS.toMillis(2);
            }
        };
        EXPORT_APPID_MAP = new LinkedHashMap<>();
    }

    private List<String> types = Collections.singletonList("processAnalyzeWorkflow");
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
    }

    @Override
    public boolean submitJob(CDLJobType cdlJobType, String jobArguments) {
        if (cdlJobType == CDLJobType.PROCESSANALYZE) {
            checkAndUpdateJobStatus(CDLJobType.PROCESSANALYZE);
            try {
                if (!systemCheck()) {
                    if (isActivityBasedPA) {
                        orchestrateJob_New();
                    } else {
                        orchestrateJob();
                    }
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
        }
        return true;
    }

    @VisibleForTesting
    boolean systemCheck() {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(appPublicUrl);
        StatusDocument statusDocument = proxy.systemCheck();
        log.info(String.format("System status is : %s.", statusDocument.getStatus()));
        return StatusDocument.UNDER_MAINTENANCE.equals(statusDocument.getStatus());
    }

    @Override
    public Date getNextInvokeTime(CustomerSpace customerSpace) {
        Tenant tenantInContext = MultiTenantContext.getTenant();
        try {
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
            MultiTenantContext.setTenant(tenant);
            CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
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
        Date currentTime = new Date(currentTimeMillis);

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
            List<WorkflowJob> runningPAJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(clusterId, types, jobStatuses);
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
                sb.append(String.format("Tenant %s run by %s. ", workflowJob.getTenant().getId(), workflowJob.getUserId()));
            }
        }
        log.info(sb.toString());

        if ((runningPAJobsCount < concurrentProcessAnalyzeJobs && autoScheduledPAJobsCount < maximumScheduledJobCount) ||
                runningPAJobsCount >= concurrentProcessAnalyzeJobs && autoScheduledPAJobsCount < minimumScheduledJobCount) {
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

    @SuppressWarnings("unchecked")
    private void orchestrateJob_New() {
        String clusterId = getCurrentClusterID();
        log.debug(String.format("Current cluster id is : %s.", clusterId));
        boolean clusterIdIsEmpty = StringUtils.isEmpty(clusterId);

        String currentBuildNumber = columnMetadataProxy.latestBuildNumber();
        log.debug(String.format("Current build number is : %s.", currentBuildNumber));

        List<SimpleDataFeed> allDataFeeds = dataFeedService.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0");
        log.info(String.format("DataFeed for active tenant count: %d.", allDataFeeds.size()));

        int runningPAJobsCount = 0;
        List<SimpleDataFeed> processAnalyzingDataFeeds = new ArrayList<>();

        List<ActivityObject> activityObjects = new ArrayList<>();
        Map<String, JobParameter> jobParameterMap = new HashMap<>();

        for (SimpleDataFeed dataFeed : allDataFeeds) {
            Tenant tenant = dataFeed.getTenant();
            if (clusterIdIsEmpty && dataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                runningPAJobsCount++;
                processAnalyzingDataFeeds.add(dataFeed);
            } else if (dataFeed.getStatus() == DataFeed.Status.Active) {
                MultiTenantContext.setTenant(tenant);
                CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
                CDLJobDetail cdlJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
                List<Action> actions = getActions();

                JobParameter jobParameter = new JobParameter();
                jobParameter.setDataFeed(dataFeed);
                jobParameter.setCdlJobDetail(cdlJobDetail);
                jobParameter.setActions(actions);
                jobParameterMap.put(tenant.getName(), jobParameter);

                ActivityObject activityObject = new ActivityObject();
                activityObject.setTenant(tenant);
                activityObject.setScheduleNow(dataFeed.isScheduleNow());
                activityObject.setScheduleTime(dataFeed.getScheduleTime());
                activityObject.setActions(actions);
                activityObject.setRetry(retryProcessAnalyze(tenant, cdlJobDetail));

                try {
                    boolean allowAutoDataCloudRefresh = batonService.isEnabled(customerSpace,
                            LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY);
                    if (allowAutoDataCloudRefresh) {
                        activityObject.setDataCloudRefresh(checkDataCloudChange(currentBuildNumber, customerSpace.toString()));
                    }
                } catch (Exception e) {
                    log.warn("get 'allow auto data cloud refresh' value failed: " + e.getMessage());
                }

                Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, cdlJobDetail);
                if (invokeTime != null) {
                    if (dataFeed.getNextInvokeTime() == null || !dataFeed.getNextInvokeTime().equals(invokeTime)) {
                        dataFeedService.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                    }
                    activityObject.setInvokeTime(invokeTime);
                }

                activityObjects.add(activityObject);
            }
        }
        PriorityQueueUtils.createOrUpdateQueue(activityObjects);
        log.info("High priority Queue : " + JsonUtils.serialize(PriorityQueueUtils.getAllMemberWithSortFromHighPriorityQueue()));
        log.info("Low priority Queue : " + JsonUtils.serialize(PriorityQueueUtils.getAllMemberWithSortFromLowPriorityQueue()));

        highMap = getMap(HIGH_PRIORITY_RUNNING_JOB_MAP);
        lowMap = getMap(LOW_PRIORITY_RUNNING_JOB_MAP);

        StringBuilder sb = new StringBuilder();
        if (clusterIdIsEmpty) {
            sb.append(String.format("Have %d running PA jobs. ", runningPAJobsCount));
            for (SimpleDataFeed dataFeed : processAnalyzingDataFeeds) {
                sb.append(String.format("Tenant %s is running PA job. ", dataFeed.getTenant().getId()));
            }
        } else {
            List<WorkflowJob> runningPAJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(clusterId, types, jobStatuses);
            runningPAJobsCount = runningPAJobs.size();

            Set<String> runningJobAppId = new HashSet<>();
            for (WorkflowJob job : runningPAJobs) {
                runningJobAppId.add(job.getApplicationId());
            }

            Set<String> notStartRunningHithTenants = new HashSet<>();
            Iterator<Map.Entry<String, JobProperty>> highMapIterator = highMap.entrySet().iterator();
            while (highMapIterator.hasNext()) {
                Map.Entry<String, JobProperty> entry = highMapIterator.next();
                JobProperty jobProperty = entry.getValue();
                if (runningJobAppId.contains(jobProperty.getApplicationId())) {
                    if (JobStatus.PENDING.equals(jobProperty.getJobStatus())) {
                        jobProperty.setJobStatus(JobStatus.RUNNING);
                    }
                } else {
                    if (JobStatus.RUNNING.equals(jobProperty.getJobStatus())) {
                        highMapIterator.remove();
                        continue;
                    } else {
                        notStartRunningHithTenants.add(entry.getKey());
                        runningPAJobsCount++;
                    }
                }
                if ((System.currentTimeMillis() - jobProperty.getStartTime() > TimeUnit.HOURS.toMillis(2)) && JobStatus.PENDING.equals(jobProperty.getJobStatus())) {
                    highMapIterator.remove();
                }
            }

            Set<String> notStartRunningLowTenants = new HashSet<>();
            Iterator<Map.Entry<String, JobProperty>> lowMapMapIterator = lowMap.entrySet().iterator();
            while (lowMapMapIterator.hasNext()) {
                Map.Entry<String, JobProperty> entry = lowMapMapIterator.next();
                JobProperty jobProperty = entry.getValue();
                if (runningJobAppId.contains(jobProperty.getApplicationId())) {
                    if (JobStatus.PENDING.equals(jobProperty.getJobStatus())) {
                        jobProperty.setJobStatus(JobStatus.RUNNING);
                    }
                } else {
                    if (JobStatus.RUNNING.equals(jobProperty.getJobStatus())) {
                        lowMapMapIterator.remove();
                        continue;
                    } else {
                        notStartRunningLowTenants.add(entry.getKey());
                        runningPAJobsCount++;
                    }
                }
                if ((System.currentTimeMillis() - jobProperty.getStartTime() > TimeUnit.HOURS.toMillis(2)) && JobStatus.PENDING.equals(jobProperty.getJobStatus())) {
                    lowMapMapIterator.remove();
                }
            }

            sb.append(String.format("Have %d running PA jobs. ", runningPAJobsCount));
            for (WorkflowJob workflowJob : runningPAJobs) {
                sb.append(String.format("Tenant %s run by %s. ", workflowJob.getTenant().getId(), workflowJob.getUserId()));
            }
            for (String tenant : notStartRunningHithTenants) {
                sb.append(String.format("Tenant %s is submitted, but not run. ", tenant));
            }
            for (String tenant : notStartRunningLowTenants) {
                sb.append(String.format("Tenant %s is submitted, but not run. ", tenant));
            }
        }
        log.info(sb.toString());

        int highPriorityRunningPAJobCount = highMap.size();
        int lowPriorityRunningPAJobCount = lowMap.size();

        String needScheduleTenantFromHighPriority = PriorityQueueUtils.pickFirstFromHighPriority();
        while (needScheduleTenantFromHighPriority != null &&
                (highMap.containsKey(needScheduleTenantFromHighPriority) || lowMap.containsKey(needScheduleTenantFromHighPriority))) {
            PriorityQueueUtils.pollFirstFromHighPriority();
            needScheduleTenantFromHighPriority = PriorityQueueUtils.pickFirstFromHighPriority();
        }
        log.info(String.format("Need to schedule high priority tenant is : %s.", needScheduleTenantFromHighPriority));

        String needScheduleTenantFromLowPriority = PriorityQueueUtils.pickFirstFromLowPriority();
        while (StringUtils.isNotEmpty(needScheduleTenantFromLowPriority) &&
                ((StringUtils.isNotEmpty(needScheduleTenantFromHighPriority) && needScheduleTenantFromHighPriority.equals(needScheduleTenantFromLowPriority)) ||
                        highMap.containsKey(needScheduleTenantFromLowPriority) ||
                        lowMap.containsKey(needScheduleTenantFromLowPriority))) {
            PriorityQueueUtils.pollFirstFromLowPriority();
            needScheduleTenantFromLowPriority = PriorityQueueUtils.pickFirstFromLowPriority();
        }
        log.info(String.format("Need to schedule low priority tenant is : %s.", needScheduleTenantFromLowPriority));

        if (StringUtils.isNotEmpty(needScheduleTenantFromHighPriority) && jobParameterMap.containsKey(needScheduleTenantFromHighPriority)) {
            if ((runningPAJobsCount < concurrentProcessAnalyzeJobs && highPriorityRunningPAJobCount < maximumHighPriorityScheduledJobCount) ||
                    runningPAJobsCount >= concurrentProcessAnalyzeJobs && highPriorityRunningPAJobCount < minimumHighPriorityScheduledJobCount) {
                JobParameter jobParameter = jobParameterMap.get(needScheduleTenantFromHighPriority);
                if (jobParameter.getDataFeed() != null && submitProcessAnalyzeJob(jobParameter.getDataFeed(),
                        jobParameter.getCdlJobDetail(), true)) {
                    log.info(String.format("Run PA  job for tenant: %s.", needScheduleTenantFromHighPriority));
                    runningPAJobsCount++;
                }
            }
        }
        if (StringUtils.isNotEmpty(needScheduleTenantFromLowPriority) && jobParameterMap.containsKey(needScheduleTenantFromLowPriority)) {
            if ((runningPAJobsCount < concurrentProcessAnalyzeJobs && lowPriorityRunningPAJobCount < maximumLowPriorityScheduledJobCount) ||
                    runningPAJobsCount >= concurrentProcessAnalyzeJobs && lowPriorityRunningPAJobCount < minimumLowPriorityScheduledJobCount) {
                JobParameter jobParameter = jobParameterMap.get(needScheduleTenantFromLowPriority);
                if (jobParameter.getDataFeed() != null && submitProcessAnalyzeJob(jobParameter.getDataFeed(),
                        jobParameter.getCdlJobDetail(),
                        false)) {
                    log.info(String.format("Run PA  job for tenant: %s.", needScheduleTenantFromLowPriority));
                }
            }
        }

        updateRedisTemplate(HIGH_PRIORITY_RUNNING_JOB_MAP, highMap);
        updateRedisTemplate(LOW_PRIORITY_RUNNING_JOB_MAP, lowMap);
    }

    private List<Action> getImportActions(List<Action> actions) {
        List<Action> importActions = new ArrayList<>();
        if (actions != null) {
            for (Action action : actions) {
                if (action.getType() == ActionType.CDL_DATAFEED_IMPORT_WORKFLOW) {
                    importActions.add(action);
                }
            }
        }
        return importActions;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Map<String, JobProperty> getMap(String key) {
        if (redisTemplate.opsForValue().get(key) == null) {
            Map<String, JobProperty> newMap = new HashMap<>();
            redisTemplate.opsForValue().set(key, newMap, 7, TimeUnit.DAYS);
            return newMap;
        }
        ObjectMapper mapper = JsonUtils.getObjectMapper();
        Map<String, JobProperty> redisMap;
        try {
            redisMap = mapper.readValue((String) redisTemplate.opsForValue().get(key),
                    new TypeReference<Map<String, JobProperty>>() {
                    });
        } catch (IOException e) {
            log.error("get map from redisCache fail.", e);
            redisMap = new HashMap<>();
        }
        return redisMap;
    }

    @VisibleForTesting
    void updateRedisTemplate(String key, Map<String, JobProperty> map) {
        redisTemplate.opsForValue().set(key, JsonUtils.serialize(map), 7, TimeUnit.DAYS);
    }

    @VisibleForTesting
    Boolean checkDataCloudChange(String currentBuildNumber, String customerSpace) {
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);

        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        String accountTableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedAccount, activeVersion);

        return (status != null
                && (status.getDataCloudBuildNumber() == null
                || DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber())
                || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                && StringUtils.isNotBlank(accountTableName));
    }


    @SuppressWarnings("unchecked")
    @VisibleForTesting
    String getCurrentClusterID() {
        String url = appPublicUrl + STACK_INFO_URL;
        String clusterId = null;
        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                    Collections.singleton(HttpServerErrorException.class), null);
            AtomicReference<Map<String, String>> stackInfo = new AtomicReference<>();
            retry.execute(retryContext -> {
                stackInfo.set(restTemplate.getForObject(url, Map.class));
                return true;
            });
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
            if ((processAnalyzeJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL) &&
                    (processAnalyzeJobDetail.getRetryCount() < processAnalyzeJobRetryCount)) {
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
        log.info(String.format("Submit process analyze job with application id: %s, tenant id: %s, retry: %s, success" +
                        " %s",
                applicationId.toString(), tenant.getName(), retry ? "y" : "n", success ? "y" : "n"));
        return true;
    }

    @VisibleForTesting
    boolean submitProcessAnalyzeJob(SimpleDataFeed dataFeed, CDLJobDetail cdlJobDetail,
                                    boolean setHighMap) {
        Tenant tenant = dataFeed.getTenant();
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
                ProcessAnalyzeRequest request;
                if (StringUtils.isNotEmpty(dataFeed.getScheduleRequest())) {
                    request = JsonUtils.deserialize(dataFeed.getScheduleRequest(), ProcessAnalyzeRequest.class);
                } else {
                    request = new ProcessAnalyzeRequest();
                    request.setUserId(USERID);
                }

                applicationId = cdlProxy.scheduleProcessAnalyze(tenant.getId(), true, request);
            }
            JobProperty jobProperty = new JobProperty();
            jobProperty.setApplicationId(applicationId.toString());
            jobProperty.setJobStatus(JobStatus.PENDING);
            jobProperty.setStartTime(System.currentTimeMillis());

            if (setHighMap) {
                highMap.put(tenant.getName(), jobProperty);
            } else {
                lowMap.put(tenant.getName(), jobProperty);
            }
            cdlJobDetail.setApplicationId(applicationId.toString());
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
        } catch (Exception e) {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            log.info(String.format("Failed to submit job for tenant name: %s", tenant.getName()));
            success = false;
        }
        cdlJobDetail.setRetryCount(retryCount);
        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        log.info(String.format("Submit process analyze job with application id: %s, tenant id: %s, retry: %s, success" +
                        " %s",
                applicationId.toString(), tenant.getName(), retry ? "y" : "n", success ? "y" : "n"));
        return true;
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
            if (dataFeed.isScheduleNow() && cdlJobType == CDLJobType.PROCESSANALYZE) {
                dataFeedService.updateDataFeedScheduleTime(customerSpace, false, null);
            }
        } else {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
            if (reachRetryLimit(cdlJobType, cdlJobDetail.getRetryCount())) {
                if (dataFeed.isScheduleNow() && cdlJobType == CDLJobType.PROCESSANALYZE) {//reach retry PA limit, do not run PA again
                    dataFeedService.updateDataFeedScheduleTime(customerSpace, false, null);
                }
                if (dataFeed.getDrainingStatus() != DrainingStatus.NONE) {
                    dataFeedService.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(), DrainingStatus.NONE.name());
                }
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

    private List<Action> getActions() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionService.findByOwnerIdAndActionStatus(null, ActionStatus.ACTIVE);
        Set<ActionType> importAndDeleteTypes = Sets.newHashSet( //
                ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, //
                ActionType.CDL_OPERATION_WORKFLOW);
        List<String> importAndDeleteJobPidStrs = actions.stream()
                .filter(action -> importAndDeleteTypes.contains(action.getType()) && action.getTrackingPid() != null)
                .map(action -> action.getTrackingPid().toString()).collect(Collectors.toList());
        List<Job> importAndDeleteJobs = workflowProxy.getWorkflowExecutionsByJobPids(importAndDeleteJobPidStrs,
                customerSpace);
        List<Long> completedImportAndDeleteJobPids = CollectionUtils.isEmpty(importAndDeleteJobs)
                ? Collections.emptyList()
                : importAndDeleteJobs.stream().filter(
                job -> job.getJobStatus() != JobStatus.PENDING && job.getJobStatus() != JobStatus.RUNNING)
                .map(Job::getPid).collect(Collectors.toList());

        List<Action> completedActions = actions.stream()
                .filter(action -> isCompleteAction(action, importAndDeleteTypes, completedImportAndDeleteJobPids))
                .collect(Collectors.toList());

        List<Action> attrManagementActions = actions.stream()
                .filter(action -> ActionType.getAttrManagementTypes().contains(action.getType()))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(attrManagementActions)) {
            completedActions.addAll(attrManagementActions);
        }

        List<Action> businessCalendarChangeActions = actions.stream()
                .filter(action -> action.getType().equals(ActionType.BUSINESS_CALENDAR_CHANGE))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(businessCalendarChangeActions)) {
            completedActions.addAll(businessCalendarChangeActions);
        }

        List<Action> ratingEngineActions = actions.stream()
                .filter(action -> action.getType() == ActionType.RATING_ENGINE_CHANGE)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(ratingEngineActions)) {
            completedActions.addAll(ratingEngineActions);
        }
        return completedActions;
    }

    private boolean isCompleteAction(Action action, Set<ActionType> selectedTypes,
                                     List<Long> completedImportAndDeleteJobPids) {
        boolean isComplete = true; // by default every action is valid
        if (selectedTypes.contains(action.getType())) {
            // special check if is selected type
            isComplete = false;
            if (completedImportAndDeleteJobPids.contains(action.getTrackingPid())) {
                isComplete = true;
            } else if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
                ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action
                        .getActionConfiguration();
                if (importActionConfiguration == null) {
                    log.error("Import action configuration is null!");
                    return false;
                }
                if (Boolean.TRUE.equals(importActionConfiguration.getMockCompleted())) {
                    isComplete = true;
                }
            }
        }
        return isComplete;
    }

    private void exportScheduleJob() {
        updateExportAppIdMap();
        List<AtlasScheduling> atlasSchedulingList =
                atlasSchedulingService.findAllByType(AtlasScheduling.ScheduleType.Export);
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
                    Long nextFireTime =
                            CronUtils.getNextFireTime(atlasScheduling.getCronExpression()).getMillis() / 1000;
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
                        if (EXPORT_APPID_MAP.containsValue(customerSpace)) {
                            continue;
                        }
                        if (submitExportJob(customerSpace, tenant)) {
                            log.info(String.format("ExportJob submitted invoke time: %s, tenant name: %s.",
                                    atlasScheduling.getPrevFireTime(),
                                    tenant.getName()));
                        }
                    }
                }
            }
        }
    }

    boolean submitExportJob(String customerSpace, Tenant tenant) {
        return submitExportJob(customerSpace, false, tenant);
    }

    @VisibleForTesting
    boolean submitExportJob(String customerSpace, boolean retry, Tenant tenant) {
        String applicationId = null;
        boolean success = true;
        if (tenant == null) {
            setMultiTenantContext(customerSpace);
        } else {
            MultiTenantContext.setTenant(tenant);
        }
        try {
            EntityExportRequest request = new EntityExportRequest();
            request.setDataCollectionVersion(dataCollectionProxy.getActiveVersion(customerSpace));
            ApplicationId tempApplicationId = cdlProxy.entityExport(customerSpace, request);
            applicationId = tempApplicationId.toString();
            if (!retry) {
                EXPORT_APPID_MAP.put(applicationId, customerSpace);
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
            List<WorkflowJob> workflowJobs = workflowProxy.queryByClusterIDAndTypesAndStatuses(clusterId, types,
                    jobStatus);
            if (CollectionUtils.isNotEmpty(workflowJobs)) {
                for (WorkflowJob workflowJob : workflowJobs) {
                    if (workflowJob.getStatus() == JobStatus.COMPLETED.getName() || workflowJob.getStatus() == JobStatus.CANCELLED.getName()) {
                        EXPORT_APPID_MAP.remove(workflowJob.getApplicationId());
                        continue;
                    }
                    if (workflowJob.getStatus() == JobStatus.FAILED.getName()) {
                        submitExportJob(EXPORT_APPID_MAP.get(workflowJob.getApplicationId()), true, null);
                        EXPORT_APPID_MAP.remove(workflowJob.getApplicationId());
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

    static class JobProperty {
        @JsonProperty
        private String applicationId;

        @JsonProperty
        private JobStatus jobStatus;

        @JsonProperty
        private long startTime;

        public String getApplicationId() {
            return applicationId;
        }

        public void setApplicationId(String applicationId) {
            this.applicationId = applicationId;
        }

        public JobStatus getJobStatus() {
            return jobStatus;
        }

        public void setJobStatus(JobStatus jobStatus) {
            this.jobStatus = jobStatus;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public String toString() {
            return "applicationId : " + applicationId + ", jobStatus : " + jobStatus + ", startTime : " + startTime;
        }
    }

    class JobParameter {
        @JsonProperty
        private SimpleDataFeed dataFeed;

        @JsonProperty
        private CDLJobDetail cdlJobDetail;

        @JsonProperty
        private List<Action> actions;

        public SimpleDataFeed getDataFeed() {
            return dataFeed;
        }

        public void setDataFeed(SimpleDataFeed dataFeed) {
            this.dataFeed = dataFeed;
        }

        public CDLJobDetail getCdlJobDetail() {
            return cdlJobDetail;
        }

        public void setCdlJobDetail(CDLJobDetail cdlJobDetail) {
            this.cdlJobDetail = cdlJobDetail;
        }

        public List<Action> getActions() {
            return actions;
        }

        public void setActions(List<Action> actions) {
            this.actions = actions;
        }
    }
}
