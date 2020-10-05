package com.latticeengines.proxy.matchapi;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.matchapi.UsageProxy;


@Lazy
@Component
public class UsageProxyImpl extends BaseRestApiProxy implements UsageProxy {

    public UsageProxyImpl() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/usage");
    }

    @Override
    public VboBatchUsageReport submitBatchReport(SubmitBatchReportRequest request) {
        String url = constructUrl("/batch");
        return post("submit batch report", url, request, VboBatchUsageReport.class);
    }

}
