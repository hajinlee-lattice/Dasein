package com.latticeengines.apps.dcp.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.DCPProject;

public interface DCPProjectRepository extends BaseJpaRepository<DCPProject, Long> {

    DCPProject findByProjectId(String projectId);
}
