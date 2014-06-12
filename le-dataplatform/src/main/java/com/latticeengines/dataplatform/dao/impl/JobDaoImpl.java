package com.latticeengines.dataplatform.dao.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Repository("jobDao")
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
        Query query = session.createQuery("from " + Job.class.getSimpleName() + " J where J.id=:aJobId");
        query.setString("aJobId", id);
        Job job = (Job) query.uniqueResult();
        return job;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Set<Job> findAllByObjectIds(Set<String> jobIds) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(Job.class, "listAllByObjectIds");
        criteria.add(Restrictions.in("id", jobIds));
        List<Job> jobs = criteria.list();
        
        return new HashSet<Job>(jobs);
    }
}
