package com.latticeengines.quartzclient.dao.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.dao.JobHistoryDao;

@Component("jobHistoryDao")
public class JobHistoryDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<JobHistory> implements JobHistoryDao {

    @Value("${quartz.scheduler.jobs.history.displaycount:5}")
    private int displayCount;

    @SuppressWarnings("rawtypes")
    @Override
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName order by PID desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.setMaxResults(displayCount);
        List list = query.list();
        List<JobHistory> jobHistories = new ArrayList<JobHistory>();
        if (list.size() == 0) {
            return null;
        } else {
            int historyCount = list.size();
            for (int i = 0; i < historyCount; i++) {
                jobHistories.add((JobHistory) list.get(i));
            }
        }
        return jobHistories;
    }

    @Override
    public void saveJobHistory(JobHistory jobHistory) {
        super.createOrUpdate(jobHistory);
    }

    @Override
    public void createJobHistory(JobHistory jobHistory) {
        super.create(jobHistory);
    }

    @Override
    public void deleteOldJobHistory(int retainingDays) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format("delete from %s where TriggeredTime < :date and TriggeredJobStatus in (:jobStatus)",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -(retainingDays));
        query.setCalendarDate("date", calendar);
        List<Integer> statusCode = new ArrayList<>();
        statusCode.add(TriggeredJobStatus.FAIL.getValue());
        statusCode.add(TriggeredJobStatus.SUCCESS.getValue());
        statusCode.add(TriggeredJobStatus.TIMEOUT.getValue());
        query.setParameterList("jobStatus", statusCode);
        query.executeUpdate();
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
                        "from %s where TenantId = :tenantId and JobName = :jobName and " +
                                "TriggeredJobStatus in (:jobStatus) order by PID desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        List<Integer> statusCode = new ArrayList<>();
        statusCode.add(TriggeredJobStatus.START.getValue());
        statusCode.add(TriggeredJobStatus.TRIGGERED.getValue());
        query.setParameterList("jobStatus", statusCode);
        query.setMaxResults(1);
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
                        "from %s where TenantId = :tenantId and JobName = :jobName and " +
                                "TriggeredJobHandle = :jobHandle order by PID desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.setString("jobHandle", triggeredJobHandle);
        query.setMaxResults(1);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (JobHistory) list.get(0);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobHistory getLastJobHistory(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName order by PID desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.setMaxResults(1);
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

    @Override
    public void deleteAllJobHistory(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobHistory> entityClz = getEntityClass();
        String queryStr = String.format("delete from %s where TenantId = :tenantId and JobName = :jobName ",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        query.executeUpdate();
    }
}
