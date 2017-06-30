package com.latticeengines.yarn.exposed.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.yarn.exposed.dao.JobDao;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;

@Component("jobEntityMgr")
public class JobEntityMgrImpl extends BaseEntityMgrImpl<Job> implements JobEntityMgr {

    @Autowired
    private JobDao jobDao;

    public JobEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<Job> getDao() {
        return jobDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Job findByObjectId(String id) {
        return jobDao.findByObjectId(id);
    }

    /**
     * find all Jobs by its object id (JobId)
     * 
     * @param <a>
     *            jobIds - job ids to find by. If argument is empty or null, a
     *            empty set is returned.
     * @return - jobs satisfying the jobids querying condition; empty Set if
     *         nothing is found.
     * 
     * 
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<Job> findAllByObjectIds(List<String> ids) {
        List<Job> jobs = new ArrayList<>();
        if (ids == null || ids.isEmpty()) {
            return jobs;
        }
        while (ids.size() > maxJobsMapping) {
            List<String> subIdList = ids.subList(0, maxJobsMapping);
            jobs.addAll(jobDao.findAllByObjectIds(subIdList));
            ids = ids.subList(maxJobsMapping, ids.size());
        }
        jobs.addAll(jobDao.findAllByObjectIds(ids));
        return jobs;
    }
}
