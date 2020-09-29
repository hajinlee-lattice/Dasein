package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;

public interface VboUsageService {

    VboBatchUsageReport submitBatchUsageReport(SubmitBatchReportRequest request);

}
