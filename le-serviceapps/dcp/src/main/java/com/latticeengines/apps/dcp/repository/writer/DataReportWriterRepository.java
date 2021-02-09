package com.latticeengines.apps.dcp.repository.writer;

import java.util.Date;
import java.util.Set;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.repository.DataReportRepository;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataReportWriterRepository extends DataReportRepository {

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.readyForRollup = true, d.refreshTime = ?2 where d.pid = ?1")
    void updateReadyForRollupToTrue(Long pid, Date refreshTime);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.readyForRollup = false, d.refreshTime = ?2 where d.pid in ?1")
    void updateReadyForRollupToFalse(Set<Long> pids, Date refreshTime);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.readyForRollup = true, d.refreshTime = ?2 where d.pid = ?1 and d" +
            ".readyForRollup != true and d.dunsCount is not null")
    int updateReadyForRollupIfNotReady(Long pid, Date refreshTime);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.basicStats = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.BasicStats basicStats);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.dunsCount = ?4, d.refreshTime = ?2, d.dataSnapshotTime=?3 WHERE d.pid = " +
            "?1")
    void updateDataReport(Long pid, Date refreshTime, Date snapshotTime, Table dunsCount);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.inputPresenceReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.InputPresenceReport inputPresenceReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.geoDistributionReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.GeoDistributionReport geoDistributionReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.matchToDUNSReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.MatchToDUNSReport matchToDUNSReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.duplicationReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.DuplicationReport duplicationReport);


    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.basicStats = ?3, d.refreshTime = ?2 WHERE d.pid = ?1 AND d.basicStats IS NULL")
    void updateDataReportIfNull(Long pid, Date refreshTime, DataReport.BasicStats basicStats);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.inputPresenceReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1 AND d.inputPresenceReport IS NULL")
    void updateDataReportIfNull(Long pid, Date refreshTime, DataReport.InputPresenceReport inputPresenceReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.geoDistributionReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1 AND d.geoDistributionReport IS NULL")
    void updateDataReportIfNull(Long pid, Date refreshTime, DataReport.GeoDistributionReport geoDistributionReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.matchToDUNSReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1 AND d.matchToDUNSReport IS NULL")
    void updateDataReportIfNull(Long pid, Date refreshTime, DataReport.MatchToDUNSReport matchToDUNSReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.duplicationReport = ?3, d.refreshTime = ?2 WHERE d.pid = ?1 AND d.duplicationReport IS NULL")
    void updateDataReportIfNull(Long pid, Date refreshTime, DataReport.DuplicationReport duplicationReport);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.dunsCount = ?4, d.refreshTime = ?2, d.dataSnapshotTime=?3 WHERE d.pid = " +
            "?1 and d.dunsCount IS NULL")
    int updateDataReportIfNull(Long pid, Date refreshTime, Date snapshotTime, Table dunsCount);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.rollupStatus = ?1 WHERE d.ownerId = ?3 AND d.level = ?2")
    int updateDataReportRollupStatus (DataReportRecord.RollupStatus status, DataReportRecord.Level level, String ownerId);
}
