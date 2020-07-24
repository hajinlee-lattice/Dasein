package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportService {

    DataReport getDataReport(DataReportRecord.Level level, String ownerId, Boolean mock);

    String rollupDataReport(DCPReportRequest request);
}
