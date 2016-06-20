package com.latticeengines.quartzclient.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.dao.JobHistoryDao;

@Component("jobHistoryDao")
public class JobHistoryDaoImpl extends BaseDaoImpl<JobHistory> implements JobHistoryDao {

    @Value("${quartz.scheduler.jobs.history.displaycount:5}")
    private int displayCount;

    @SuppressWarnings("rawtypes")
    @Override
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName order by TriggeredTime desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        List list = query.list();
        List<JobHistory> jobHistories = new ArrayList<JobHistory>();
        if (list.size() == 0) {
            return null;
        } else {
            int historyCount = list.size() > displayCount ? displayCount : list.size();
            for (int i = 0; i < historyCount; i++) {
                jobHistories.add((JobHistory) list.get(i));
            }
        }
        return jobHistories;
    }

    @Override
    public void saveJobHistory(JobHistory jobHistory) {
        super.create(jobHistory);
    }

    @Override
    protected Class<JobHistory> getEntityClass() {
        return JobHistory.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName and TriggeredJobStatus = :jobStatus order by TriggeredTime desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.setInteger("jobStatus", TriggeredJobStatus.START.getValue());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (JobHistory) list.get(0);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName and TriggeredJobHandle = :jobHandle order by TriggeredTime desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.setString("jobHandle", triggeredJobHandle);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (JobHistory) list.get(0);
        }
    }

    @Override
    public void updateJobHistory(JobHistory jobHistory) {
        super.update(jobHistory);
    }
}
