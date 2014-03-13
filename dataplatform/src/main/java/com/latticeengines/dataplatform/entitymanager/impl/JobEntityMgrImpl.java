package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Job;

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
    public Set<Job> getByIds(Set<String> jobIds) {
        return jobDao.getByJobIds(jobIds);
    }

}
