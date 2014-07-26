package com.latticeengines.dataplatform.entitymanager;

import java.util.List;
import com.latticeengines.domain.exposed.dataplatform.Job;

public interface JobEntityMgr extends BaseEntityMgr<Job> {

    // Set<Job> getByIds(Set<String> jobIds);
    Job findByObjectId(String id);

    List<Job> findAllByObjectIds(List<String> jobIds);
}
