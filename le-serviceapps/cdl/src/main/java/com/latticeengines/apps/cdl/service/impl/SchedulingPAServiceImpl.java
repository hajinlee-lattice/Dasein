package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AutoScheduleTenantActivity;
import com.latticeengines.domain.exposed.cdl.DataCloudRefreshTenantActivity;
import com.latticeengines.domain.exposed.cdl.SchedulingPAQueue;
import com.latticeengines.domain.exposed.cdl.RetryTenantActivity;
import com.latticeengines.domain.exposed.cdl.ScheduleNowTenantActivity;
import com.latticeengines.domain.exposed.cdl.SystemStatus;
import com.latticeengines.domain.exposed.cdl.TenantActivity;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("SchedulingPAService")
public class SchedulingPAServiceImpl implements SchedulingPAService {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAServiceImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private ActionService actionService;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Value("${cdl.processAnalyze.maximum.priority.large.account.count}")
    private long largeAccountCountLimit;

    @Value("${cdl.processAnalyze.job.retry.count:1}")
    private int processAnalyzeJobRetryCount;

    @Value("${cdl.processAnalyze.maximum.priority.schedulenow.job.count}")
    private int maxScheduleNowJobCount;

    @Value("${cdl.processAnalyze.maximum.priority.large.job.count}")
    private int maxLargeJobCount;

    @Value("${cdl.processAnalyze.concurrent.job.count}")
    private int concurrentProcessAnalyzeJobs;

    private List<SchedulingPAQueue> schedulingPAQueues;

    private List<TenantActivity> tenantActivityList;

    private SystemStatus systemStatus;

    @Override
    public void setSystemStatus() {

        int runningTotalCount = 0;
        int runningScheduleNowCount = 0;
        int runningLargeJobCount = 0;

        Set<String> largeJobTenantId = new HashSet<>();
        Set<String> runningPATenantId = new HashSet<>();

        tenantActivityList = new LinkedList<>();
        List<DataFeed> allDataFeeds = dataFeedService.getDataFeeds(TenantStatus.ACTIVE, "4.0");
        log.info(String.format("DataFeed for active tenant count: %d.", allDataFeeds.size()));
        String currentBuildNumber = columnMetadataProxy.latestBuildNumber();
        log.debug(String.format("Current build number is : %s.", currentBuildNumber));

        for (DataFeed simpleDataFeed : allDataFeeds) {
            if (simpleDataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                runningTotalCount++;
                if (simpleDataFeed.isScheduleNow()) {
                    runningScheduleNowCount++;
                }
                String customerSpace = simpleDataFeed.getTenant().getId();
                runningPATenantId.add(customerSpace);
                if (isLarge(customerSpace)) {
                    runningLargeJobCount++;
                }
            } else if (!DataFeed.Status.RUNNING_STATUS.contains(simpleDataFeed.getStatus())) {
                DataFeedExecution execution;
                try {
                    execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(simpleDataFeed,
                            DataFeedExecutionJobType.PA);
                } catch (Exception e) {
                    execution = null;
                }
                Tenant tenant = simpleDataFeed.getTenant();
                MultiTenantContext.setTenant(tenant);
                if (retryProcessAnalyze(execution)) {
                    TenantActivity retryPriorityObject = new RetryTenantActivity();
                    retryPriorityObject.setTenantId(tenant.getId());
                    retryPriorityObject.setLastFinishTime(execution.getUpdated().getTime());
                    retryPriorityObject.setIsLarge(isLarge(MultiTenantContext.getShortTenantId()));
                    tenantActivityList.add(retryPriorityObject);
                    if (retryPriorityObject.isLarge()) {
                        largeJobTenantId.add(tenant.getId());
                    }
                } else {
                    if (simpleDataFeed.isScheduleNow()) {
                        TenantActivity scheduleNowPriorityObject =
                                new ScheduleNowTenantActivity(tenant.getTenantType());
                        scheduleNowPriorityObject.setTenantId(tenant.getId());
                        scheduleNowPriorityObject.setScheduleTime(simpleDataFeed.getScheduleTime().getTime());
                        scheduleNowPriorityObject.setIsLarge(isLarge(MultiTenantContext.getShortTenantId()));
                        tenantActivityList.add(scheduleNowPriorityObject);
                        if (scheduleNowPriorityObject.isLarge()) {
                            largeJobTenantId.add(tenant.getId());
                        }
                        continue;
                    }
                    List<Action> actions = getActions();
                    if (CollectionUtils.isNotEmpty(actions)) {
                        Long firstActionTime = new Date().getTime();
                        Long lastActionTime = 0L;
                        for (Action action : actions) {
                            Long actionTime = action.getCreated().getTime();
                            if (firstActionTime - actionTime > 0) {
                                firstActionTime = actionTime;
                            }
                            if (actionTime - lastActionTime > 0) {
                                lastActionTime = actionTime;
                            }
                        }
                        log.info("tenant " + tenant.getName() + " firstActionTime = " + firstActionTime + ", " +
                                "lastActionTime = " + lastActionTime);
                        Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, execution);
                        if (invokeTime != null) {
                            if (simpleDataFeed.getNextInvokeTime() == null || !simpleDataFeed.getNextInvokeTime().equals(invokeTime)) {
                                dataFeedService.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                            }
                            AutoScheduleTenantActivity autoSchedulePriorityObject =
                                    new AutoScheduleTenantActivity(tenant.getTenantType());
                            autoSchedulePriorityObject.setTenantId(tenant.getId());
                            autoSchedulePriorityObject.setInvokeTime(invokeTime.getTime());
                            autoSchedulePriorityObject.setFirstActionTime(firstActionTime);
                            autoSchedulePriorityObject.setLastActionTime(lastActionTime);
                            if(autoSchedulePriorityObject.isValid()) {
                                autoSchedulePriorityObject.setIsLarge(isLarge(MultiTenantContext.getShortTenantId()));
                                tenantActivityList.add(autoSchedulePriorityObject);
                                if (autoSchedulePriorityObject.isLarge()) {
                                    largeJobTenantId.add(tenant.getId());
                                }
                                continue;
                            }
                        }
                    }
                    DataCollectionStatus status =
                            dataCollectionProxy.getOrCreateDataCollectionStatus(MultiTenantContext.getShortTenantId(), null);
                    if (isDataCloudRefresh(tenant, currentBuildNumber, status)) {
                        DataCloudRefreshTenantActivity dataCloudRefreshPriorityObject =
                                new DataCloudRefreshTenantActivity(tenant.getTenantType());
                        dataCloudRefreshPriorityObject.setTenantId(tenant.getId());
                        dataCloudRefreshPriorityObject.setIsLarge(isLarge(status));
                        tenantActivityList.add(dataCloudRefreshPriorityObject);
                        if (dataCloudRefreshPriorityObject.isLarge()) {
                            largeJobTenantId.add(tenant.getId());
                        }
                    }
                }
            }
        }
        int canRunJobCount = concurrentProcessAnalyzeJobs -runningTotalCount;
        int canRunScheduleNowJobCount = maxScheduleNowJobCount - runningScheduleNowCount;
        int canRunLargeJobCount = maxLargeJobCount - runningLargeJobCount;

        systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(canRunJobCount);
        systemStatus.setCanRunLargeJobCount(canRunLargeJobCount);
        systemStatus.setCanRunScheduleNowJobCount(canRunScheduleNowJobCount);
        systemStatus.setRunningTotalCount(runningTotalCount);
        systemStatus.setRunningLargeJobCount(runningLargeJobCount);
        systemStatus.setRunningScheduleNowCount(runningScheduleNowCount);
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        systemStatus.setRunningPATenantId(runningPATenantId);
        log.info("running PA tenant is : " + JsonUtils.serialize(runningPATenantId));
        log.info("running PA job count : " + runningTotalCount);
        log.info("running ScheduleNow PA job count : " + runningScheduleNowCount);
        log.info("running large PA job count : " + runningLargeJobCount);
        log.info("priority Queue : " + JsonUtils.serialize(showQueue()));
        log.info("large PA Job tenant is : " + JsonUtils.serialize(largeJobTenantId));
    }

    public void initQueue() {
        SchedulingPAQueue<RetryTenantActivity> retrySchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<ScheduleNowTenantActivity> customerScheduleNowSchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<AutoScheduleTenantActivity> customerAutoScheduleSchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<DataCloudRefreshTenantActivity> customerDataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<ScheduleNowTenantActivity> nonCustomerScheduleNowSchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<AutoScheduleTenantActivity> nonCustomerAutoScheduleSchedulingPAQueue = new SchedulingPAQueue<>();
        SchedulingPAQueue<DataCloudRefreshTenantActivity> nonCustomerDataCloudRefreshSchedulingPAQueue = new SchedulingPAQueue<>();
    }

    @Override
    public Boolean isLargeTenant(String tenantId) {
        return systemStatus.getLargeJobTenantId().contains(tenantId);
    }

    @Override
    public List<String> getRunningPATenantId() {
        return new ArrayList<>(systemStatus.getRunningPATenantId());
    }

    @Override
    public Map<String, List<String>> showQueue() {
        Map<String, List<String>> queueMap = new HashMap<>();
        queueMap.put("retrySchedulingPAQueue", retrySchedulingPAQueue.getAll());
        queueMap.put("customerScheduleNowSchedulingPAQueue", customerScheduleNowSchedulingPAQueue.getAll());
        queueMap.put("customerAutoScheduleSchedulingPAQueue", customerAutoScheduleSchedulingPAQueue.getAll());
        queueMap.put("customerDataCloudRefreshSchedulingPAQueue", customerDataCloudRefreshSchedulingPAQueue.getAll());
        queueMap.put("nonCustomerScheduleNowSchedulingPAQueue", nonCustomerScheduleNowSchedulingPAQueue.getAll());
        queueMap.put("nonCustomerAutoScheduleSchedulingPAQueue", nonCustomerAutoScheduleSchedulingPAQueue.getAll());
        queueMap.put("nonCustomerDataCloudRefreshSchedulingPAQueue", nonCustomerDataCloudRefreshSchedulingPAQueue.getAll());
        return queueMap;
    }

    @Override
    public String getPositionFromQueue(String tenantName) {
        int index = retrySchedulingPAQueue.getPosition(tenantName);
        String queueName = "";
        if (index == -1) {
            index = customerScheduleNowSchedulingPAQueue.getPosition(tenantName);
            if (index == -1) {
                index = customerAutoScheduleSchedulingPAQueue.getPosition(tenantName);
                if (index == -1) {
                    index = customerDataCloudRefreshSchedulingPAQueue.getPosition(tenantName);
                    if (index == -1) {
                        index = nonCustomerScheduleNowSchedulingPAQueue.getPosition(tenantName);
                        if (index == -1) {
                            index = nonCustomerAutoScheduleSchedulingPAQueue.getPosition(tenantName);
                            if (index == -1) {
                                index = nonCustomerDataCloudRefreshSchedulingPAQueue.getPosition(tenantName);
                                if (index != -1) {
                                     queueName = "nonCustomerDataCloudRefreshSchedulingPAQueue";
                                }
                            } else {
                                queueName = "nonCustomerAutoScheduleSchedulingPAQueue";
                            }
                        } else {
                            queueName = "nonCustomerScheduleNowSchedulingPAQueue";
                        }
                    } else {
                        queueName = "customerDataCloudRefreshSchedulingPAQueue";
                    }
                } else {
                    queueName = "customerAutoScheduleSchedulingPAQueue";
                }
            } else {
                queueName = "customerScheduleNowSchedulingPAQueue";
            }
        } else {
            queueName = "retrySchedulingPAQueue";
        }
        if (index != -1) {
            return String.format("tenant %s at Queue %s Position %d", tenantName, queueName, index);
        } else {
            return "cannot find this tenant " + tenantName + " in Queue.";
        }
    }

    private boolean isLarge(String customerSpace) {
        DataCollectionStatus status =
                dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null);
        return isLarge(status);
    }

    private boolean isLarge(DataCollectionStatus status) {
        DataCollectionStatusDetail detail = status.getDetail();
        return detail.getAccountCount() > largeAccountCountLimit;
    }

    private boolean retryProcessAnalyze(DataFeedExecution execution) {
        if ((execution != null) && (DataFeedExecution.Status.Failed.equals(execution.getStatus()))) {
            if (!reachRetryLimit(CDLJobType.PROCESSANALYZE, execution.getRetryCount())) {
                return true;
            } else {
                log.info("Tenant exceeds retry limit and skip failed exeuction");
            }
        }
        return false;
    }

    private boolean reachRetryLimit(CDLJobType cdlJobType, int retryCount) {
        switch (cdlJobType) {
            case PROCESSANALYZE:
                return retryCount >= processAnalyzeJobRetryCount;
            default:
                return false;
        }
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
            if ((execution.getStatus() == DataFeedExecution.Status.Failed) &&
                    (execution.getRetryCount() < processAnalyzeJobRetryCount)) {
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
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        String accountTableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedAccount, activeVersion);

        return (status != null
                && (status.getDataCloudBuildNumber() == null
                || DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber())
                || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                && StringUtils.isNotBlank(accountTableName));
    }

    private Boolean isDataCloudRefresh(Tenant tenant, String currentBuildNumber, DataCollectionStatus status) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
            boolean allowAutoDataCloudRefresh =  batonService.isEnabled(customerSpace,
                    LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY);
            if (allowAutoDataCloudRefresh) {

                return checkDataCloudChange(currentBuildNumber, customerSpace.toString(), status);
            }
        } catch (Exception e) {
            log.warn("get 'allow auto data cloud refresh' value failed: " + e.getMessage());
        }
        return false;
    }

    private Boolean isCustomer(TenantType tenantType) {
        return tenantType == TenantType.CUSTOMER;
    }
}
