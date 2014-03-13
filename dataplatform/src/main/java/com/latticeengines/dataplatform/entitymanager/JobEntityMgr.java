package com.latticeengines.dataplatform.entitymanager;

import java.util.Set;

import com.latticeengines.dataplatform.exposed.domain.Job;

public interface JobEntityMgr extends BaseEntityMgr<Job> {

    Set<Job> getByIds(Set<String> jobIds);
}
