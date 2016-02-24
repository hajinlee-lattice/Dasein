package com.latticeengines.quartz.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartz.dao.JobHistoryDao;

@Component("jobHistoryDao")
public class JobHistoryDaoImpl extends BaseDaoImpl<JobHistory> implements JobHistoryDao {

    @Value("${quartz.scheduler.jobs.history.displaycount:5}")
    private int displayCount;

    @SuppressWarnings("rawtypes")
    @Override
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        Session session = getSession();
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
        Session session = getSession();
        session.persist(jobHistory);
        session.flush();
        session.close();
    }

    @Override
    protected Class<JobHistory> getEntityClass() {
        return JobHistory.class;
    }

    private Session getSession() throws HibernateException {
        Session sess = null;
        try {
            sess = getSessionFactory().getCurrentSession();
        } catch (HibernateException e) {
            sess = getSessionFactory().openSession();
        }
        return sess;
    }

}
