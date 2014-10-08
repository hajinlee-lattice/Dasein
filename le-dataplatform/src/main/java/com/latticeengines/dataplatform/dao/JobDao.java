package com.latticeengines.dataplatform.dao;

import java.util.List;
import com.latticeengines.domain.exposed.dataplatform.Job;

public interface JobDao extends BaseDao<Job> {

    // Set<Job> getByJobIds(Set<String> jobIds);
    Job findByObjectId(String id);

    List<Job> findAllByObjectIds(List<String> ids);
}
