package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectRepository extends BaseJpaRepository<Project, Long> {

    Project findByProjectId(String projectId);

    @Query("SELECT p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, p.teamId" +
            " FROM Project AS p WHERE p.projectId = ?1")
    List<Object[]> findProjectInfoByProjectId(String projectId);

    @Query("SELECT p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, p.teamId" +
            " FROM Project AS p")
    List<Object[]> findAllProjects(Pageable pageable);

    @Query("SELECT p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, p.teamId" +
            " FROM Project AS p JOIN ProjectSystemLink AS ps ON p.pid = ps.project JOIN DataFeedTask AS dft ON ps.pid = dft.importSystem" +
            " WHERE dft.sourceId = ?1")
    List<Object[]> findProjectInfoBySourceId(String sourceId);

    @Query("SELECT p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, p.teamId" +
            " FROM Project AS p WHERE p.teamId IN (?1) OR p.teamId IS null")
    List<Object[]> findProjectsInTeamIds(List<String> teamIds, Pageable pageable);

    @Query("SELECT p.projectId, p.projectDisplayName, p.rootPath, p.deleted, p.created, p.updated, p.createdBy, p.recipientList, p.teamId" +
            " FROM Project AS p WHERE p.projectId = ?1 AND (p.teamId IN (?2) OR p.teamId IS null)")
    List<Object[]> findProjectInfoByProjectIdInTeamIds(String projectId, List<String> teamIds);
}
