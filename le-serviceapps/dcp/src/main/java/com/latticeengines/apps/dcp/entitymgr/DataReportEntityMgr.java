package com.latticeengines.apps.dcp.entitymgr;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataReportEntityMgr extends BaseEntityMgrRepository<DataReportRecord, Long> {

    DataReportRecord findDataReportRecord(DataReportRecord.Level level, String ownerId);

    DataReport.BasicStats findDataReportBasicStats(DataReportRecord.Level level, String ownerId);

    Map<String, DataReport.BasicStats> findDataReportBasicStatsByLevel(DataReportRecord.Level level);

    Map<String, DataReport.BasicStats> findBasicStatsByParentLevelAndOwnerId(DataReportRecord.Level parentLevel,
                                                                             String parentOwnerId);

    Set<String> getChildrenIds(DataReportRecord.Level level, String ownerId, Boolean readyForRollup);

    List<Object[]> findPidAndDunsCountTableName(DataReportRecord.Level level, String ownerId);

    int countSiblingsByParentLevelAndOwnerId(DataReportRecord.Level level, String ownerId);

    boolean existsDataReport(DataReportRecord.Level level, String ownerId);

    Long findDataReportPid(DataReportRecord.Level level, String ownerId);

    Long findParentId(Long pid);

    void updateDataReportRecord(Long pid, boolean readyForRollup);

    void updateDataReportRecordIfNotReady(Long pid, boolean readyForRollup);

    void uploadDataReportRecord(Long pid, Table dunsCountTable, Date snapshotTime);

    void updateDataReportRecord(Long pid, DataReport.BasicStats basicStats);

    void updateDataReportRecord(Long pid, DataReport.InputPresenceReport inputPresenceReport);

    void updateDataReportRecord(Long pid, DataReport.GeoDistributionReport geoDistributionReport);

    void updateDataReportRecord(Long pid, DataReport.MatchToDUNSReport matchToDUNSReport);

    void updateDataReportRecord(Long pid, DataReport.DuplicationReport duplicationReport);

    void updateDataReportRecordIfNull(Long pid, DataReport.BasicStats basicStats);

    void updateDataReportRecordIfNull(Long pid, DataReport.InputPresenceReport inputPresenceReport);

    void updateDataReportRecordIfNull(Long pid, DataReport.GeoDistributionReport geoDistributionReport);

    void updateDataReportRecordIfNull(Long pid, DataReport.MatchToDUNSReport matchToDUNSReport);

    void updateDataReportRecordIfNull(Long pid, DataReport.DuplicationReport duplicationReport);

}
