package com.latticeengines.dataplatform.dao;

import java.util.Set;

import com.latticeengines.dataplatform.exposed.domain.Job;

public interface JobDao extends BaseDao<Job> {

    Set<Job> getByJobIds(Set<String> jobIds);


}
