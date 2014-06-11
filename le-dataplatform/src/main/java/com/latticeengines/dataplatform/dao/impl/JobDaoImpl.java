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

import com.latticeengines.common.exposed.util.JsonUtils;
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
    public Job deserialize(String id, String content) {
        Job job = JsonUtils.deserialize(content, Job.class);
        if (job != null) {
            job.setId(id);
        }

        return job;
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

    /*
     * @SuppressWarnings("unchecked")
     * 
     * @Override public Set<Job> getByJobIds(Set<String> ids) { Set<Job> jobs =
     * new HashSet<Job>(); for (Iterator<String> it = (Iterator<String>)
     * getStore().getKeys(); it.hasNext();) { String key = it.next(); if
     * (ids.contains(key)) { jobs.add(deserialize(key, (String)
     * getStore().getProperty(key))); } } return jobs; }
     */

}
