package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;

public interface VboUsageService {

    VboBatchUsageReport submitBatchUsageReport(SubmitBatchReportRequest request);

}
