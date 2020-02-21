package com.latticeengines.apps.dcp.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectRepository extends BaseJpaRepository<Project, Long> {

    Project findByProjectId(String projectId);
}
