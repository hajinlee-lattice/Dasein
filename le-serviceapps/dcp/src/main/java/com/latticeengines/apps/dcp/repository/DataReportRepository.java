package com.latticeengines.apps.dcp.repository;

import java.util.List;
import java.util.Set;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportRepository extends BaseJpaRepository<DataReportRecord, Long> {

    DataReportRecord findByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    boolean existsByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    @Query("select count(*) from DataReportRecord d join DataReportRecord d2 on d.parentId = d2.pid " +
            "where d2.level = ?1 AND d2.ownerId = ?2 AND d.readyForRollup = true")
    int countSiblingsByParentLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    @Query("select d.pid,dc.name from DataReportRecord as d left join d.dunsCount as dc WHERE d.level = ?1 AND " +
            "d.ownerId = ?2")
    List<Object[]> findPidAndDunsCountTableName(DataReportRecord.Level level, String ownerId);

    @Query("SELECT d.pid from DataReportRecord d WHERE d.level = ?1 AND d.ownerId = ?2")
    Long findPidByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    @Query("SELECT d.parentId from DataReportRecord d WHERE d.pid = ?1")
    Long findParentIdByPid(Long pid);

    @Query("SELECT d.pid from DataReportRecord d WHERE d.parentId = ?1")
    Set<Long> findPidsByParentId(Long parentId);

    @Query("SELECT d.basicStats from DataReportRecord d WHERE d.level = ?1 AND d.ownerId = ?2")
    DataReport.BasicStats findBasicStatsByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    @Query("SELECT d.ownerId, d.basicStats from DataReportRecord d WHERE d.level = ?1")
    List<Object[]> findBasicStatsByLevel(DataReportRecord.Level level);

    @Query("select d.ownerId, d.basicStats from DataReportRecord d join DataReportRecord d2 on d.parentId = d2.pid " +
            "where d2.level = ?1 AND d2.ownerId = ?2")
    List<Object[]> findBasicStatsByParentLevelAndOwnerId(DataReportRecord.Level parentLevel, String parentOwnerId);

    @Query("select d.ownerId from DataReportRecord d join DataReportRecord d2 on d.parentId = d2.pid " +
            "where d2.level = ?1 AND d2.ownerId = ?2 and d.readyForRollup = true")
    Set<String> findChildrenIdsByParentLevelAndOwnerId(DataReportRecord.Level parentLevel, String parentOwnerId);

    DataReportRecord findByLevelAndOwnerIdAndReadyForRollup(DataReportRecord.Level level, String ownerId, boolean readyForRollup);

    @Query(value = "SELECT ownerId, refreshTime from DataReportRecord where level = ?1 order by " +
            "refreshTime")
    List<Object[]> findOwnerIdAndRefreshDate(DataReportRecord.Level level, Pageable page);
}
