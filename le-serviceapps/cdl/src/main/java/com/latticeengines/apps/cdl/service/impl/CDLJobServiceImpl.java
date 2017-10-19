package com.latticeengines.apps.cdl.service.impl;

import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.apps.cdl.entitymanager.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("cdlJobService")
public class CDLJobServiceImpl implements CDLJobService {

    private static final Logger log = LoggerFactory.getLogger(CDLJobServiceImpl.class);

    @Autowired
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private CDLProxy cdlProxy;

    @Value("${cdl.consolidate.concurrent.job.count:4}")
    private int concurrentConsolidateJobs;

    @Value("${cdl.profile.concurrent.job.count:2}")
    private int concurrentProfileJobs;

    @Value("${cdl.consolidate.job.interval.days:1}")
    private int consolidateJobInterval;

    @Value("${cdl.profile.job.interval.days:7}")
    private int profileJobInterval;

    @Value("${cdl.consolidate.job.retry.count:2}")
    private int consolidateJobRetryCount;

    @Value("${cdl.profile.job.retry.count:2}")
    private int profileJobRetryCount;

    @Override
    public boolean submitJob(CDLJobType cdlJobType, String jobArguments) {
        if (cdlJobType == CDLJobType.IMPORT) {
            submitImportJob(jobArguments);
        } else if (cdlJobType == CDLJobType.ORCHESTRATION) {
            int runningConsolidateJobs = checkAndUpdateJobStatus(CDLJobType.CONSOLIDATE);
            int runningProfileJobs = checkAndUpdateJobStatus(CDLJobType.PROFILE);
            orchestrateJob(jobArguments, runningConsolidateJobs, runningProfileJobs);
        }
        return true;
    }

    private void orchestrateJob(String jobArguments, int runningConsolidateJobs, int runningProfileJobs) {
        List<DataFeed> allDataFeeds = dataFeedProxy.getAllDataFeeds();
        for (DataFeed dataFeed : allDataFeeds) {
            MultiTenantContext.setTenant(dataFeed.getTenant());
            if (dataFeed != null && dataFeed.isAutoScheduling()) {
                CDLJobDetail consolidateJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(
                        CDLJobType.CONSOLIDATE);
                CDLJobDetail profileJobDetail = cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROFILE);
                if (runningConsolidateJobs < concurrentConsolidateJobs
                        && meetConsolidateRule(dataFeed, consolidateJobDetail, profileJobDetail)) {
                    submitConsolidateJob(jobArguments, dataFeed.getTenant(), consolidateJobDetail);
                    runningConsolidateJobs++;
                } else if (runningProfileJobs < concurrentProfileJobs
                        && meetProfileRule(dataFeed, consolidateJobDetail, profileJobDetail)) {
                    submitProfileJob(jobArguments, dataFeed.getTenant(), profileJobDetail);
                    runningProfileJobs++;
                }
            }
        }
    }

    private int checkAndUpdateJobStatus(CDLJobType cdlJobType) {
        List<CDLJobDetail> details =cdlJobDetailEntityMgr.listAllRunningJobByJobType(cdlJobType);
        int runningJobs = details.size();
        for (CDLJobDetail cdlJobDetail : details) {
            String appId = cdlJobDetail.getApplicationId();
            if (!StringUtils.isEmpty(appId)) {
                Job job = workflowProxy.getWorkflowJobFromApplicationId(appId);
                if (job != null && !job.isRunning()) {
                    JobStatus jobStatus = job.getJobStatus();
                    DataFeed dataFeed = dataFeedProxy.getDataFeed(cdlJobDetail.getTenant().getId());
                    if (jobStatus == JobStatus.COMPLETED) {
                        cdlJobDetail.setCdlJobStatus(CDLJobStatus.COMPLETE);
                        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
                        if (dataFeed.getDrainingStatus() == DrainingStatus.DRAINING_CONSOLIDATE) {
                            dataFeedProxy.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(),
                                    DrainingStatus.DRAINING_PROFILE.name());
                        } else if (dataFeed.getDrainingStatus() == DrainingStatus.DRAINING_PROFILE) {
                            dataFeedProxy.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(),
                                    DrainingStatus.NONE.name());
                        }
                    } else {
                        cdlJobDetail.setCdlJobStatus(CDLJobStatus.FAIL);
                        cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
                        if (reachRetryLimit(cdlJobType, cdlJobDetail.getRetryCount())) {
                            if (dataFeed.getDrainingStatus() != DrainingStatus.NONE) {
                                dataFeedProxy.updateDataFeedDrainingStatus(cdlJobDetail.getTenant().getId(),
                                        DrainingStatus.NONE.name());
                            }
                        }

                    }
                    runningJobs--;
                }
            }
        }
        return runningJobs;
    }

    private boolean reachRetryLimit(CDLJobType cdlJobType, int retryCount) {
        switch (cdlJobType) {
            case CONSOLIDATE:
                return retryCount >= consolidateJobRetryCount;
            case PROFILE:
                return retryCount >= profileJobRetryCount;
            default:
                return false;
        }
    }

    private boolean meetConsolidateRule(DataFeed dataFeed, CDLJobDetail consolidateJobDetail,
                                        CDLJobDetail profileJobDetail) {
        //Check if data feed status is right.
        if (!dataFeed.getStatus().isAllowConsolidation()) {
            return false;
        }
        //Consolidate need to run first.
        if (consolidateJobDetail == null) {
            return true;
        } else {
            if (dataFeed.getDrainingStatus() == DrainingStatus.DRAINING_CONSOLIDATE) {
                if (!consolidateJobDetail.isRunning()) {
                    if (profileJobDetail == null || !profileJobDetail.isRunning()) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                if (!consolidateJobDetail.isRunning()) {
                    if (profileJobDetail != null && profileJobDetail.isRunning()) {
                        return false;
                    } else {
                        if (Date.from(Instant.now()).getTime() > (consolidateJobDetail.getLastUpdateDate().getTime() +
                                TimeUnit.DAYS.toMillis(consolidateJobInterval))) {
                            return true;
                        } else {
                            if (consolidateJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL &&
                                    consolidateJobDetail.getRetryCount() < consolidateJobRetryCount) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    }
                } else {
                    return false;
                }

            }
        }
    }

    private boolean meetProfileRule(DataFeed dataFeed, CDLJobDetail consolidateJobDetail,
                                    CDLJobDetail profileJobDetail) {
        //Consolidate need to run first.
        if (!dataFeed.getStatus().isAllowProfile()) {
            return false;
        }
        if (consolidateJobDetail == null) {
            return false;
        } else {
            if (consolidateJobDetail.isRunning()) {
                return false;
            } else {
                if (profileJobDetail == null) {
                    return true;
                } else {
                    if (profileJobDetail.isRunning()) {
                        return false;
                    } else {
                        if (dataFeed.getDrainingStatus() == DrainingStatus.DRAINING_PROFILE) {
                            return true;
                        } else {
                            if (Date.from(Instant.now()).getTime() > (profileJobDetail.getLastUpdateDate().getTime() +
                                    TimeUnit.DAYS.toMillis(profileJobInterval))) {
                                return true;
                            } else {
                                if (profileJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL &&
                                        profileJobDetail.getRetryCount() < profileJobRetryCount) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void submitConsolidateJob(String jobArguments, Tenant tenant, CDLJobDetail cdlJobDetail) {
        if (cdlJobDetail != null
                && cdlJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL
                && cdlJobDetail.getRetryCount() < consolidateJobRetryCount) {
            cdlJobDetail.setRetryCount(cdlJobDetail.getRetryCount() + 1);
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            cdlJobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.CONSOLIDATE, tenant);
        }
        log.info(String.format("Submit consolidate job with job detail id: %d", cdlJobDetail.getPid()));
        orchestrateJob(cdlJobDetail);
    }

    private void submitProfileJob(String jobArguments, Tenant tenant, CDLJobDetail cdlJobDetail) {
        if (cdlJobDetail != null
                && cdlJobDetail.getCdlJobStatus() == CDLJobStatus.FAIL
                && cdlJobDetail.getRetryCount() < profileJobRetryCount) {
            cdlJobDetail.setRetryCount(cdlJobDetail.getRetryCount() + 1);
            cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
            cdlJobDetailEntityMgr.updateJobDetail(cdlJobDetail);
        } else {
            cdlJobDetail = cdlJobDetailEntityMgr.createJobDetail(CDLJobType.PROFILE, tenant);
        }
        log.info(String.format("Submit profile job with job detail id: %d", cdlJobDetail.getPid()));
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
        switch (cdlJobDetail.getCdlJobType()) {
            case CONSOLIDATE:
                return cdlProxy.consolidate(cdlJobDetail.getTenant().getId());
            case PROFILE:
                return cdlProxy.profile(cdlJobDetail.getTenant().getId());
            default:
                return null;
        }
    }

    private void submitImportJob(String jobArguments) {

    }
}
