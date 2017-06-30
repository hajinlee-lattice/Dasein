package com.latticeengines.yarn.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.Job;

public interface JobDao extends BaseDao<Job> {

    // Set<Job> getByJobIds(Set<String> jobIds);
    Job findByObjectId(String id);

    List<Job> findAllByObjectIds(List<String> ids);
}
