package com.latticeengines.workflow.entitymanager.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.dao.WorkflowJobDao;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

@Component("workflowJobEntityMgr")
public class WorkflowJobEntityMgrImpl extends BaseEntityMgrImpl<WorkflowJob> implements WorkflowJobEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobEntityMgrImpl.class);

    @Autowired
    private WorkflowJobDao workflowJobDao;

    @Override
    public BaseDao<WorkflowJob> getDao() {
        return workflowJobDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(WorkflowJob workflowJob) {
        super.create(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByWorkflowPid(long workflowPid) {
        return workflowJobDao.findByWorkflowPid(workflowPid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByApplicationId(String applicationId) {
        return workflowJobDao.findByApplicationId(applicationId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByWorkflowId(long workflowId) {
        return workflowJobDao.findByWorkflowId(workflowId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds) {
        if (CollectionUtils.isEmpty(workflowIds)) {
            return Collections.emptyList();
        }
        return workflowJobDao.findByWorkflowIds(workflowIds);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsAndTypes(List<Long> workflowIds, List<String> types) {
        return workflowJobDao.findByWorkflowIds(workflowIds, types);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobId(List<Long> workflowIds, List<String> types,
                                                                   Long parentJobId) {
        return findByWorkflowIdsOrTypesOrParentJobId(workflowIds, types, null, parentJobId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsOrTypesOrParentJobId(List<Long> workflowIds, List<String> types,
            List<String> statuses, Long parentJobId) {
        return workflowJobDao.findByWorkflowIds(workflowIds, types, statuses, parentJobId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowPids(List<Long> workflowPids) {
        if (CollectionUtils.isEmpty(workflowPids)) {
            return Collections.emptyList();
        }
        return workflowJobDao.findByWorkflowPids(workflowPids);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowPidsAndTypes(List<Long> workflowPids, List<String> types) {
        return workflowJobDao.findByWorkflowPids(workflowPids, types);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowPidsOrTypesOrParentJobId(List<Long> workflowPids, List<String> types,
                                                                    Long parentJobId) {
        if (workflowPids != null && types != null && parentJobId != null) {
            return workflowJobDao.findByWorkflowPids(workflowPids, types, parentJobId);
        } else if (workflowPids != null && types != null) {
            return workflowJobDao.findByWorkflowPids(workflowPids, types);
        } else if (workflowPids != null && parentJobId != null) {
            return workflowJobDao.findByWorkflowPids(workflowPids, parentJobId);
        } else if (workflowPids != null) {
            return workflowJobDao.findByWorkflowPids(workflowPids);
        } else if (types != null && parentJobId != null) {
            return workflowJobDao.findByTypes(types, parentJobId);
        } else if (types != null) {
            return workflowJobDao.findByTypes(types);
        } else {
            return null;
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByTenantAndWorkflowPids(Tenant tenant, List<Long> workflowPids) {
        return workflowJobDao.findByTenantAndWorkflowPids(tenant, workflowPids);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public void updateWorkflowJob(WorkflowJob workflowJob) {
        workflowJobDao.update(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public WorkflowJob updateStatusFromYarn(WorkflowJob workflowJob,
                                            com.latticeengines.domain.exposed.dataplatform.JobStatus yarnJobStatus) {
        JobStatus jobStatus = JobStatus.fromString(yarnJobStatus.getStatus().name(), yarnJobStatus.getState());
        if (jobStatus != null) {
            workflowJob.setStatus(jobStatus.name());
        } else {
            log.warn("Unknown job status. YarnJobStatus = " + JsonUtils.serialize(yarnJobStatus));
        }

        int ret = workflowJob.setStartTimeInMillis(yarnJobStatus.getStartTime());
        if (ret != 0) {
            log.warn("StartTime is null. YarnJobStatus = " + JsonUtils.serialize(yarnJobStatus));
        }

        workflowJobDao.updateStatusAndStartTime(workflowJob);
        return workflowJob;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateWorkflowJobStatus(WorkflowJob workflowJob) {
        workflowJobDao.updateStatus(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateParentJobId(WorkflowJob workflowJob) {
        workflowJobDao.updateParentJobId(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void registerWorkflowId(WorkflowJob workflowJob) {
        workflowJobDao.registerWorkflowId(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateReport(WorkflowJob workflowJob) {
        workflowJobDao.updateReport(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateOutput(WorkflowJob workflowJob) {
        workflowJobDao.updateOutput(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateErrorDetails(WorkflowJob workflowJob) {
        workflowJobDao.updateErrorDetails(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateApplicationId(WorkflowJob workflowJob) {
        workflowJobDao.updateApplicationId(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateApplicationIdAndEmrClusterId(WorkflowJob workflowJob) {
        workflowJobDao.updateApplicationIdAndEmrClusterId(workflowJob);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public WorkflowJob deleteByApplicationId(String applicationId) {
        return workflowJobDao.deleteByApplicationId(applicationId);
    }
}
