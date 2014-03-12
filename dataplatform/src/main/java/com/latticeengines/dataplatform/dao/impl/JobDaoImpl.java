package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.dataplatform.exposed.domain.Job;

@Component("jobDao")
public class JobDaoImpl extends BaseDaoImpl<Job> implements JobDao {

    public JobDaoImpl() {
        super();
    }

    @Override
    public Job deserialize(String id, String content) {
        Job job = new Job();
        job.setId(id);
        return job;
    }

    @Override
    public String serialize(Job entity) {
        return "";
    }

}
