package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
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
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
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

    @Value("${cdl.processAnalyze.concurrent.job.count:2}")
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
        log.info("starting submit job");
        if (cdlJobType == CDLJobType.IMPORT) {
            log.info("starting submit import job");
            submitImportJob(jobArguments);
            log.info("end submit import job");
        } else if (cdlJobType == CDLJobType.PROCESSANALYZE) {
            log.info("starting submit process analyze job");
            checkAndUpdateJobStatus(CDLJobType.PROCESSANALYZE);
            try {
                orchestrateJob();
            } catch (Exception e) {
                log.error(e.getMessage());
                throw e;
            }
            log.info("end submit process analyze job");
        }
        log.info("end submit job");
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
            int invokeHour = zkConfigService.getInvokeTime(customerSpace);
            log.info(String.format("configured invoke hour: %d", invokeHour));

            Tenant tenantInContext = MultiTenantContext.getTenant();
            try {
                MultiTenantContext.setTenant(tenant);
                invokeTime = getInvokeTime(processAnalyzeJobDetail, invokeHour, new Date(tenant.getRegisteredTime()));
                if (invokeTime != null) {
                    log.info(String.format("next invoke time for %s: %s", customerSpace.getTenantId(), invokeTime.toString()));
                }
            } finally {
                MultiTenantContext.setTenant(tenantInContext);
            }
        }
        return invokeTime;
    }

    private void orchestrateJob() {
        List<SimpleDataFeed> allDataFeeds = dataFeedProxy.getAllSimpleDataFeeds();
        log.info(String.format("data feeds count: %d", allDataFeeds.size()));

        int runningProcessAnalyzeJobs = 0;
        for (SimpleDataFeed dataFeed : allDataFeeds) {
            if (dataFeed.getStatus() == DataFeed.Status.ProcessAnalyzing) {
                Tenant tenant = dataFeed.getTenant();
                log.info(String.format("ProcessAnalyzing tenant : %s", tenant.getId()));
                runningProcessAnalyzeJobs++;
            }
        }

        if (runningProcessAnalyzeJobs >= concurrentProcessAnalyzeJobs) {
            return;
        }

        List<Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>>> list = new ArrayList<>();
        long currentTimeMillis = System.currentTimeMillis();
        log.info(String.format("current time: %s", (new Date(currentTimeMillis)).toString()));

        for (SimpleDataFeed dataFeed : allDataFeeds) {
            if (dataFeed.getStatus() != DataFeed.Status.Active) {
                continue;
            }

            Tenant tenant = dataFeed.getTenant();
            MultiTenantContext.setTenant(tenant);
            log.info(String.format("tenant: %s", tenant.getId()));
            CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
            Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()), tenant, processAnalyzeJobDetail);
            if (invokeTime!= null) {
                if (dataFeed.getNextInvokeTime() == null || !dataFeed.getNextInvokeTime().equals(invokeTime)) {
                    try {
                        log.info(String.format("update next invoke time for s%.", tenant.getId()));
                        dataFeedProxy.updateDataFeedNextInvokeTime(tenant.getId(), invokeTime);
                    } catch (Exception e) {
                        log.error(String.format("update next invoke time for s% failed.", tenant.getId()));
                        log.error(e.getMessage());
                    }
                }
                if (currentTimeMillis > invokeTime.getTime()) {
                    log.info(String.format("next invoke time for %s: %s", tenant.getId(), invokeTime.toString()));
                    list.add(new HashMap.SimpleEntry<>(invokeTime,
                            new HashMap.SimpleEntry<>(dataFeed, processAnalyzeJobDetail)));
                }
            }
        }

        list.sort(Comparator.comparing(Map.Entry::getKey));
        log.info(String.format("need to submit process analyze jobs count: %d", list.size()));

        for (Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>> entry : list) {
            SimpleDataFeed dataFeed = entry.getValue().getKey();
            CDLJobDetail processAnalyzeJobDetail = entry.getValue().getValue();
            if (runningProcessAnalyzeJobs < concurrentProcessAnalyzeJobs)
            {
                if (submitProcessAnalyzeJob(dataFeed.getTenant(), processAnalyzeJobDetail)) {
                    runningProcessAnalyzeJobs++;
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
        log.info("start check and update job status");
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
        log.info("end check and update job status");
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
                applicationId = cdlProxy.processAnalyze(tenant.getId(), null);
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
                log.info("Tenant %s exceeds retry limit and skip failed exeuction", tenant.getName());
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
            log.info("update job detail status to completed");
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.COMPLETE);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            log.info("update job detail status to fail");
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
            if (reachRetryLimit(cdlJobType, cdlJobDetail.getRetryCount())) {
                log.info("reach retry limit");
                if (dataFeed.getDrainingStatus() != DrainingStatus.NONE) {
                    dataFeedProxy.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(),
                            DrainingStatus.NONE.name());
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

}
