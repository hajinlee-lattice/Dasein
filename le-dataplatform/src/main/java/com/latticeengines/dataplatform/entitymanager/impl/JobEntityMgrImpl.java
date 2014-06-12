package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.Job;

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
    public Job findByObjectId(String id) {
        return jobDao.findByObjectId(id);
    }

    
    /**
     * find all Jobs by its object id (JobId)
     * 
     * @param <a> jobIds - job ids to find by.   If argument is empty or null, a empty set is returned.
     * @return - jobs satisfying the jobids querying condition;  empty Set if nothing is found.
     * 
     *  
     */
    @Override
    public Set<Job> findAllByObjectIds(Set<String> jobIds) {
        if(jobIds == null || jobIds.isEmpty())
            return new HashSet<Job>();
                    
        return jobDao.findAllByObjectIds(jobIds);
    }
}
