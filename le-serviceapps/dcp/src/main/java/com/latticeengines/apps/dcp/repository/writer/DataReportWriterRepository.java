package com.latticeengines.apps.dcp.repository.writer;

import java.util.Date;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.repository.DataReportRepository;
import com.latticeengines.domain.exposed.dcp.DataReport;

public interface DataReportWriterRepository extends DataReportRepository {

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE DataReportRecord d SET d.basicStats = ?3, d.refreshTime = ?2 WHERE d.pid = ?1")
    void updateDataReport(Long pid, Date refreshTime, DataReport.BasicStats basicStats);

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
}
