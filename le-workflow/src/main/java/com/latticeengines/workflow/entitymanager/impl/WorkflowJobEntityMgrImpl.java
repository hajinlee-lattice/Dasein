package com.latticeengines.workflow.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.workflow.exposed.dao.WorkflowJobDao;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

@Component("workflowJobEntityMgr")
public class WorkflowJobEntityMgrImpl extends BaseEntityMgrImpl<WorkflowJob> implements WorkflowJobEntityMgr {

    @Autowired
    private WorkflowJobDao workflowJobDao;

    @Override
    public BaseDao<WorkflowJob> getDao() {
        return workflowJobDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(WorkflowJob workflowJob) {
        super.create(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByApplicationId(String applicationId) {
        return workflowJobDao.findByApplicationId(applicationId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByWorkflowId(long workflowId) {
        return workflowJobDao.findByWorkflowId(workflowId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds) {
        return workflowJobDao.findByWorkflowIds(workflowIds);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIds(List<Long> workflowIds, List<String> types) {
        return workflowJobDao.findByWorkflowIds(workflowIds, types);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public WorkflowJob findByWorkflowIdWithFilter(long workflowId) {
        return workflowJobDao.findByWorkflowId(workflowId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsWithFilter(List<Long> workflowIds) {
        return workflowJobDao.findByWorkflowIds(workflowIds);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsWithFilter(List<Long> workflowIds, List<String> types) {
        return workflowJobDao.findByWorkflowIds(workflowIds, types);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByWorkflowIdsWithFilter(List<Long> workflowIds, List<String> types, Long parentJobId) {
        return workflowJobDao.findByWorkflowIds(workflowIds, types, parentJobId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByTenant(Tenant tenant) {
        return workflowJobDao.findByTenant(tenant);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByTenant(Tenant tenant, List<String> types) {
        return workflowJobDao.findByTenant(tenant, types);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<WorkflowJob> findByTenantAndWorkflowIds(Tenant tenant, List<Long> workflowIds) {
        return workflowJobDao.findByTenantAndWorkflowIds(tenant, workflowIds);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateWorkflowJob(WorkflowJob workflowJob) {
        workflowJobDao.update(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public WorkflowJob updateStatusFromYarn(WorkflowJob workflowJob,
                                            com.latticeengines.domain.exposed.dataplatform.JobStatus yarnJobStatus) {
        workflowJob.setStatus(JobStatus.fromString(yarnJobStatus.getStatus().name(), yarnJobStatus.getState()).name());
        workflowJob.setStartTimeInMillis(yarnJobStatus.getStartTime());
        workflowJobDao.updateStatusAndStartTime(workflowJob);
        return workflowJob;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateWorkflowJobStatus(WorkflowJob workflowJob) {
        workflowJobDao.updateStatus(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateParentJobId(WorkflowJob workflowJob) {
        workflowJobDao.updateParentJobId(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void registerWorkflowId(WorkflowJob workflowJob) {
        workflowJobDao.registerWorkflowId(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateReport(WorkflowJob workflowJob) {
        workflowJobDao.updateReport(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateOutput(WorkflowJob workflowJob) {
        workflowJobDao.updateOutput(workflowJob);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateErrorDetails(WorkflowJob workflowJob) {
        workflowJobDao.updateErrorDetails(workflowJob);
    }
}
