package com.latticeengines.quartzclient.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.quartz.JobActive;
import com.latticeengines.quartzclient.dao.JobActiveDao;

@Component("jobActiveDao")
public class JobActiveDaoImpl extends BaseDaoImpl<JobActive> implements
        JobActiveDao {

    @SuppressWarnings("rawtypes")
    @Override
    public boolean getJobActive(String jobName, String tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<JobActive> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where TenantId = :tenantId and JobName = :jobName",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantId", tenantId);
        query.setString("jobName", jobName);
        List list = query.list();
        if (list.size() == 0) {
            return false;
        } else {
            JobActive jobActive = (JobActive) list.get(0);
            return jobActive.getIsActive();
        }
    }

    @Override
    protected Class<JobActive> getEntityClass() {
        return JobActive.class;
    }

}
