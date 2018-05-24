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
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
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
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${cdl.processAnalyze.concurrent.job.count:2}")
    private int concurrentProcessAnalyzeJobs;

    @Value("${cdl.processAnalyze.job.retry.count:0}")
    private int processAnalyzeJobRetryCount;

    @Value("${common.adminconsole.url:}")
    private String quartzMicroserviceHostPort;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    private CDLProxy cdlProxy;

    @PostConstruct
    public void initialize() {
        if (isQuartzStack()) {
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
            int runningProcessAnalyzeJobs = checkAndUpdateJobStatus(CDLJobType.PROCESSANALYZE);
            try {
                orchestrateJob(runningProcessAnalyzeJobs);
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
                Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
                MultiTenantContext.setTenant(tenant);
                CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
                Date create_date = processAnalyzeJobDetail == null ? null : processAnalyzeJobDetail.getCreateDate();
                invokeTime = getInvokeTime(invokeHour, create_date, new Date(tenant.getRegisteredTime()));
                log.info(String.format("next invoke time for %s: %s", customerSpace.getTenantId(), invokeTime.toString()));
            } finally {
                MultiTenantContext.setTenant(tenantInContext);
            }
        }
        return invokeTime;
    }

    private void orchestrateJob(int runningProcessAnalyzeJobs) {
        List<SimpleDataFeed> allDataFeeds = dataFeedProxy.getAllSimpleDataFeeds();
        log.info(String.format("data feeds count: %d", allDataFeeds.size()));

        List<Map.Entry<Date, Map.Entry<SimpleDataFeed, CDLJobDetail>>> list = new ArrayList<>();
        long currentTimeMillis = System.currentTimeMillis();
        log.info(String.format("current time: %s", (new Date(currentTimeMillis)).toString()));

        for (SimpleDataFeed dataFeed : allDataFeeds) {
            Tenant tenant = dataFeed.getTenant();
            MultiTenantContext.setTenant(tenant);
            log.info(String.format("tenant: %s", tenant.getId()));
            if (runningProcessAnalyzeJobs < concurrentProcessAnalyzeJobs) {
                CDLJobDetail processAnalyzeJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE);
                if (processAnalyzeJobDetail == null || processAnalyzeJobDetail.getCdlJobStatus() != CDLJobStatus.RUNNING) {
                    Date invokeTime = getNextInvokeTime(CustomerSpace.parse(tenant.getId()));
                    if (invokeTime!= null && currentTimeMillis > invokeTime.getTime()) {
                        log.info(String.format("next invoke time for %s: %s", tenant.getId(), invokeTime.toString()));
                        list.add(new HashMap.SimpleEntry<>(invokeTime,
                                new HashMap.SimpleEntry<>(dataFeed, processAnalyzeJobDetail)));
                    }
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
                if (meetProcessAnalyzeRule(dataFeed, processAnalyzeJobDetail)) {
                    submitProcessAnalyzeJob(dataFeed.getTenant(), processAnalyzeJobDetail);
                    runningProcessAnalyzeJobs++;
                    log.info(String.format("submitted invoke time: %s, tenant name: %s", entry.getKey(),
                            dataFeed.getTenant().getName()));
                }
            } else {
                break;
            }
        }
    }

    private Date getInvokeTime(int invokeHour, Date createTime, Date tenantCreateDate) {
        Calendar calendar = Calendar.getInstance();
        if (createTime == null) {
            calendar.setTime(new Date(System.currentTimeMillis()));

            calendar.set(Calendar.HOUR_OF_DAY, invokeHour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            if (calendar.getTime().before(tenantCreateDate)) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
            }
        } else {
            calendar.setTime(createTime);
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

    private boolean meetProcessAnalyzeRule(SimpleDataFeed dataFeed, CDLJobDetail processAnalyzeJobDetail) {
        log.info(String.format("data feed status: %s", dataFeed.getStatus().toString()));
        if (dataFeed.getStatus() == DataFeed.Status.Initing || dataFeed.getStatus() == DataFeed.Status.Initialized) {
            return false;
        }
        if (processAnalyzeJobDetail == null) {
            log.info(String.format("process analyze job is null"));
            return true;
        } else {
            if (processAnalyzeJobDetail.isRunning()) {
                log.info(String.format("process analyze job is running"));
                return false;
            } else if (processAnalyzeJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL) {
                if (processAnalyzeJobDetail.getRetryCount() < processAnalyzeJobRetryCount) {
                    log.info(String.format("verify retry count, return true"));
                    return true;
                } else {
                    log.info(String.format("verify retry count, return false"));
                    return false;
                }
            } else {
                log.info(String.format("process analyze job is complete"));
                return true;
            }
        }
    }

    private void submitProcessAnalyzeJob(Tenant tenant, CDLJobDetail cdlJobDetail) {
        if (cdlJobDetail != null
                && cdlJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL
                && cdlJobDetail.getRetryCount() < processAnalyzeJobRetryCount) {
            cdlJobDetail.setRetryCount(cdlJobDetail.getRetryCount() + 1);
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            cdlJobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROCESSANALYZE, tenant);
        }
        log.info(String.format("Submit process analyze job with job detail id: %d", cdlJobDetail.getPid()));
        orchestrateJob(cdlJobDetail);
    }

    private boolean orchestrateJob(CDLJobDetail cdlJobDetail) {
        log.info("start orchestrate job");
        MultiTenantContext.setTenant(cdlJobDetail.getTenant());
        try {
            ApplicationId applicationId = startApplication(cdlJobDetail);
            cdlJobDetail.setApplicationId(applicationId.toString());
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } catch (Exception e) {
            log.error("Orchestrate job failed");
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
            return false;
        }
        return true;
    }

    private ApplicationId startApplication(CDLJobDetail cdlJobDetail) {
        ProcessAnalyzeRequest request = null;
        switch (cdlJobDetail.getCdlJobType()) {
            case PROCESSANALYZE:
                return cdlProxy.processAnalyze(cdlJobDetail.getTenant().getId(), request);
            default:
                return null;
        }
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

    private boolean isQuartzStack() {
        return StringUtils.isNotBlank(System.getenv(LE_STACK))
                && System.getenv(LE_STACK).equalsIgnoreCase(QUARTZ_STACK);
    }
}