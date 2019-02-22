package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("cdlJobService")
public class CDLJobServiceImpl implements CDLJobService {

    private static final Logger log = LoggerFactory.getLogger(CDLJobServiceImpl.class);

    private static final String LE_STACK = "LE_STACK";
    private static final String QUARTZ_STACK = "quartz";
    private static final String USERID = "Auto Scheduled";

    @Inject
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${cdl.processAnalyze.concurrent.job.count:4}")
    private int concurrentProcessAnalyzeJobs;

    @Value("${cdl.processAnalyze.job.retry.count:1}")
    private int processAnalyzeJobRetryCount;

    @Value("${common.adminconsole.url:}")
    private String quartzMicroserviceHostPort;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${common.quartz.stack.flag:false}")
    private boolean isQuartzStack;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    private CDLProxy cdlProxy;

    private List<String> types = Collections.singletonList("ProcessAnalyzeWorkflow");
    private List<String> jobStatuses = Collections.singletonList(JobStatus.RUNNING.getName());

    @PostConstruct
    public void initialize() {
        if (isQuartzStack) {
            cdlProxy = new CDLProxy(quartzMicroserviceHostPort);
            log.info(String.format("CDLJobService running on quartz stack with cdlHostPort=%s, dataFeedHostPort=%s, workflowHostPort=%s",
                    cdlProxy.getHostport(), dataFeedProxy.getHostport(), workflowProxy.getHostport()));
        } else {
            cdlProxy = new CDLProxy(microserviceHostPort);
            log.info(String.format("CDLJobService running with cdlHostPort=%s, dataFeedHostPort=%s, workflowHostPort=%s",
                    cdlProxy.getHostport(), dataFeedProxy.getHostport(), workflowProxy.getHostport()));
        }
    }

    @Override
    public boolean submitJob(CDLJobType cdlJobType, String jobArguments) {
        if (cdlJobType == CDLJobType.IMPORT) {
            submitImportJob(jobArguments);
        } else if (cdlJobType == CDLJobType.PROCESSANALYZE) {
            checkAndUpdateJobStatus(CDLJobType.PROCESSANALYZE);
            try {
                orchestrateJob();
            } catch (Exception e) {
                log.error("orchestrateJob CDLJobType.PROCESSANALYZE failed" + e);
                throw e;
            }
        }
        return true;
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

    private Date getNextInvokeTime(CustomerSpace customerSpace, Tenant tenant, CDLJobDetail processAnalyzeJobDetail) {
        Date invokeTime = null;

        boolean allowAutoSchedule = false;
        try {
            allowAutoSchedule = batonService.isEnabled(customerSpace,LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
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
        List<SimpleDataFeed> allDataFeeds = dataFeedProxy.getAllSimpleDataFeeds();
        long currentTimeMillis = System.currentTimeMillis();
        Date currentTime = new Date(currentTimeMillis);

        int runningProcessAnalyzeJobs = 0;
        boolean haveAutoScheduledPAJob = false;
        List<SimpleDataFeed> processAnalyzingDataFeeds = new ArrayList<SimpleDataFeed>();
        List<Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>>> list = new ArrayList<>();
        for (SimpleDataFeed dataFeed : allDataFeeds) {
            Tenant tenant = dataFeed.getTenant();
            if (tenant.getStatus() == TenantStatus.ACTIVE) {
                if (dataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                    runningProcessAnalyzeJobs++;
                    processAnalyzingDataFeeds.add(dataFeed);

                    if (!haveAutoScheduledPAJob) {
                        haveAutoScheduledPAJob = isAutoScheduledPAJob(tenant.getId());
                    }
                } else if (dataFeed.getStatus() == DataFeed.Status.Active) {
                    MultiTenantContext.setTenant(tenant);
                    CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
                    Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, processAnalyzeJobDetail);
                    if (invokeTime != null) {
                        if (dataFeed.getNextInvokeTime() == null || dataFeed.getNextInvokeTime().before(currentTime)) {
                            dataFeedProxy.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                        }
                        if (currentTimeMillis > invokeTime.getTime()) {
                            list.add(new HashMap.SimpleEntry<>(invokeTime,
                                    new HashMap.SimpleEntry<>(dataFeed, processAnalyzeJobDetail)));
                        }
                    }
                }
            } else {
                if (dataFeed.getNextInvokeTime() != null) {
                    dataFeedProxy.updateDataFeedNextInvokeTime(tenant.getId(), null);
                }
            }
        }

        for (SimpleDataFeed dataFeed : processAnalyzingDataFeeds) {
            log.info(String.format("Tenant %s is running PA job.", dataFeed.getTenant().getName()));
        }

        if (runningProcessAnalyzeJobs >= concurrentProcessAnalyzeJobs && haveAutoScheduledPAJob) {
            log.info("Running process analyze jobs count is more than concurrent process analyze jobs count.");
            return;
        }

        list.sort(Comparator.comparing(Map.Entry::getKey));
        for (Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>> entry : list) {
            SimpleDataFeed dataFeed = entry.getValue().getKey();
            CDLJobDetail processAnalyzeJobDetail = entry.getValue().getValue();
            if (runningProcessAnalyzeJobs < concurrentProcessAnalyzeJobs || !haveAutoScheduledPAJob)
            {
                if (submitProcessAnalyzeJob(dataFeed.getTenant(), processAnalyzeJobDetail)) {
                    runningProcessAnalyzeJobs++;
                    haveAutoScheduledPAJob = true;
                    log.info(String.format("submitted invoke time: %s, tenant name: %s", entry.getKey(),
                             dataFeed.getTenant().getName()));
                }
            } else {
                break;
            }
        }
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
                (processAnalyzeJobDetail.getRetryCount() <= processAnalyzeJobRetryCount)) {
                calendar.setTime(processAnalyzeJobDetail.getLastUpdateDate());
                calendar.add(Calendar.HOUR_OF_DAY, 2);
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
        List<CDLJobDetail> details =cdlJobDetailEntityMgr.listAllRunningJobByJobType(cdlJobType);
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

    private boolean submitProcessAnalyzeJob(Tenant tenant, CDLJobDetail cdlJobDetail) {
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
                applicationId = cdlProxy.restartProcessAnalyze(tenant.getId());
            } else {
                ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
                request.setUserId(USERID);

                applicationId = cdlProxy.processAnalyze(tenant.getId(), request);
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
        log.info(String.format("Submit process analyze job with job detail id: %d, retry: %s, success %s",
                               cdlJobDetail.getPid(), retry ? "y" : "n", success ? "y" : "n"));
        return true;
    }

    private boolean retryProcessAnalyze(Tenant tenant, CDLJobDetail cdlJobDetail) {
        DataFeedExecution execution = null;
        try {
            DataFeed dataFeed = dataFeedEntityMgr.findDefaultFeed();
            execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(dataFeed,
                DataFeedExecutionJobType.PA);
        } catch (Exception e) {
            execution = null;
        }
        if ((execution != null) && (DataFeedExecution.Status.Failed.equals(execution.getStatus()))) {
            if ((cdlJobDetail == null) || (cdlJobDetail.getRetryCount() < processAnalyzeJobRetryCount)) {
                return true;
            } else {
                log.info(String.format("Tenant %s exceeds retry limit and skip failed exeuction", tenant.getName()));
            }
        }
        return false;
    }

    private void submitImportJob(String jobArguments) {

    }

    private void updateOneJobStatus(CDLJobType cdlJobType, CDLJobDetail cdlJobDetail, Job job) {
        JobStatus jobStatus = job.getJobStatus();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(cdlJobDetail.getTenant().getId());
        if (jobStatus == JobStatus.COMPLETED) {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.COMPLETE);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
            if (reachRetryLimit(cdlJobType, cdlJobDetail.getRetryCount()) &&
                    dataFeed.getDrainingStatus() != DrainingStatus.NONE) {
                dataFeedProxy.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(), DrainingStatus.NONE.name());
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

    private boolean isAutoScheduledPAJob (String tenantId) {
        List<Job> jobs = workflowProxy.getJobs(null, types, jobStatuses, false, tenantId);
        if(jobs != null) {
            if (jobs.size() == 1) {
                return USERID.equals(jobs.get(0).getUser());
            } else {
                log.warn(String.format("There are more than one running PA jobs for tenant %s", tenantId));
            }
        }
        return false;
    }
}
