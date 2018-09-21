package com.latticeengines.yarn.exposed.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.yarn.exposed.dao.JobDao;

@Component("jobDao")
public class JobDaoImpl extends BaseDaoImpl<Job> implements JobDao {

    public JobDaoImpl() {
        super();
    }

    @Override
    protected Class<Job> getEntityClass() {
        return Job.class;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Job findByObjectId(String id) {
        Session session = sessionFactory.getCurrentSession();
        Query<Job> query = session.createQuery("from " + Job.class.getSimpleName() + " J where J.id=:aJobId",
                Job.class);
        query.setParameter("aJobId", id);
        Job job = (Job) query.uniqueResult();
        return job;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<Job> findAllByObjectIds(List<String> ids) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(Job.class, "listAllByObjectIds");
        criteria.add(Restrictions.in("id", ids));
        criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        List<Job> jobs = criteria.list();
        return jobs;
    }
}
