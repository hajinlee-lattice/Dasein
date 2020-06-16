package com.latticeengines.apps.dcp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportEntityMgr extends BaseEntityMgrRepository<DataReportRecord, Long> {

    DataReportRecord findDataReportRecord(DataReportRecord.Level level, String ownerId);

    DataReport.BasicStats findDataReportBasicStats(DataReportRecord.Level level, String ownerId);

    boolean existsDataReport(DataReportRecord.Level level, String ownerId);

    Long findDataReportPid(DataReportRecord.Level level, String ownerId);

    void updateDataReportRecord(Long pid, DataReport.BasicStats basicStats);

    void updateDataReportRecord(Long pid, DataReport.InputPresenceReport inputPresenceReport);

    void updateDataReportRecord(Long pid, DataReport.GeoDistributionReport geoDistributionReport);

    void updateDataReportRecord(Long pid, DataReport.MatchToDUNSReport matchToDUNSReport);

    void updateDataReportRecord(Long pid, DataReport.DuplicationReport duplicationReport);

}
