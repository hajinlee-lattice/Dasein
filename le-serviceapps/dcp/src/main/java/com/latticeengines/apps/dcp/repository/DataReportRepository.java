package com.latticeengines.apps.dcp.repository;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportRepository extends BaseJpaRepository<DataReportRecord, Long> {

    DataReportRecord findByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    boolean existsByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    @Query("SELECT d.pid from DataReportRecord d WHERE d.level = ?1 AND d.ownerId = ?2")
    Long findPidByLevelAndOwnerId(DataReportRecord.Level level, String ownerId);
}
