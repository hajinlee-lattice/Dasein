package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectRepository extends BaseJpaRepository<Project, Long> {

    @Query("SELECT pjc from Project pjc WHERE pjc.projectId = :projectId AND pjc.deleted != TRUE")
    Project findByProjectId(@Param("projectId") String projectId);

    @Query("SELECT pjc from Project pjc WHERE pjc.importSystem = :importSystem AND pjc.deleted != TRUE")
    Project findByImportSystem(@Param("importSystem") S3ImportSystem importSystem);

    List<Project> findByDeletedNot(Boolean deleted);
}
