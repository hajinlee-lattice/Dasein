package com.latticeengines.proxy.exposed.matchapi;

import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;

public interface UsageProxy {

    VboBatchUsageReport submitBatchReport(SubmitBatchReportRequest request);

}
