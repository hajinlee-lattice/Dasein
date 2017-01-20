package com.latticeengines.quartzclient.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.quartz.JobSource;
import com.latticeengines.quartzclient.dao.JobSourceDao;

@Component("jobSourceDao")
public class JobSourceDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<JobSource> implements JobSourceDao {
    @Override
    protected Class<JobSource> getEntityClass() {
        return JobSource.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobSource getJobSourceType(String tenantId, String jobName) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobSource> entityClz = getEntityClass();
        String queryStr = String.format(
                        "from %s where TenantId = :tenantId and JobName = :jobName",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (JobSource) list.get(0);
        }
    }

    @Override
    public void saveJobSource(JobSource jobSource) {
        super.createOrUpdate(jobSource);
    }
}
