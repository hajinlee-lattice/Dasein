package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectRepository extends BaseJpaRepository<Project, Long> {

    Project findByProjectId(String projectId);

    Project findByImportSystem(S3ImportSystem importSystem);

    @Query("select p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, s.pid" +
            " from Project as p join p.importSystem as s where p.projectId = ?1")
    List<Object[]> findProjectInfoByProjectId(String projectId);

    @Query("select p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, s.pid" +
            " from Project as p join p.importSystem as s")
    List<Object[]> findAllProjects();

    @Query("select p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, s.pid" +
            " from Project as p join p.importSystem as s join DataFeedTask as dft on s.pid = dft.importSystem" +
            " where dft.sourceId = ?1")
    List<Object[]> findProjectInfoBySourceId(String sourceId);

    @Query("select s.* from Project as p join p.importSystem as s where p.projectId = ?1")
    S3ImportSystem findImportSystemByProjectId(String projectId);

}
