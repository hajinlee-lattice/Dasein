package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportService {

    DataReport getDataReport(String customerSpace, DataReportRecord.Level level, String ownerId);

    DataReportRecord getDataReportRecord(String customerSpace, DataReportRecord.Level level, String ownerId);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport dataReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.BasicStats basicStats);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.InputPresenceReport inputPresenceReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.GeoDistributionReport geoDistributionReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.MatchToDUNSReport matchToDUNSReport);

    void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId, DataReport.DuplicationReport duplicationReport);
}
