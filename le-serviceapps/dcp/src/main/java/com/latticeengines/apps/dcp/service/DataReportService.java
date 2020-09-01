package com.latticeengines.apps.dcp.service;

import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;

public interface DataReportService {

    DataReport getDataReport(String customerSpace, DataReportRecord.Level level, String ownerId);

    DataReport.BasicStats getDataReportBasicStats(String customerSpace, DataReportRecord.Level level, String ownerId);

    Map<String, DataReport.BasicStats> getDataReportBasicStats(String customerSpace, DataReportRecord.Level level);

    Map<String, DataReport.BasicStats> getDataReportBasicStatsByParent(String customerSpace,
                                                                       DataReportRecord.Level parentLevel,
                                                                       String parentOwnerId);

    DataReportRecord getDataReportRecord(String customerSpace, DataReportRecord.Level level, String ownerId);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport dataReport);

    void updateReadyForRollup(String customerSpace, DataReportRecord.Level level, String ownerId);

    void registerDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId,
                      DunsCountCache cache);

    Set<String> getChildrenIds(String customerSpace, DataReportRecord.Level level, String ownerId);

    DunsCountCache getDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.BasicStats basicStats);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.InputPresenceReport inputPresenceReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.GeoDistributionReport geoDistributionReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.MatchToDUNSReport matchToDUNSReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.DuplicationReport duplicationReport);
}
