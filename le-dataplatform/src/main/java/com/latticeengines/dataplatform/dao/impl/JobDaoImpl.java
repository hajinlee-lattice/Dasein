package com.latticeengines.dataplatform.dao.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Component("jobDao")
public class JobDaoImpl extends BaseDaoImpl<Job> implements JobDao {

    public JobDaoImpl() {
        super();
    }

    @Override
    public Job deserialize(String id, String content) {
        Job job = JsonUtils.deserialize(content, Job.class);
        if (job != null) {
            job.setId(id);
        }
        
        return job;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Job> getByJobIds(Set<String> ids) {
        Set<Job> jobs = new HashSet<Job>();
        for (Iterator<String> it = (Iterator<String>) getStore().getKeys(); it.hasNext();) {
            String key = it.next();
            if (ids.contains(key)) {
                jobs.add(deserialize(key, (String) getStore().getProperty(key)));
            }
        }
        return jobs;
    }
}
