package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

@Component("testDataReportProxy")
public class TestDataReportProxy extends PlsRestApiProxyBase {

    public TestDataReportProxy() {
        super("pls/datareport");
    }

    public DataReport getDataReport(DataReportRecord.Level level, String ownerId) {
        String url = constructUrl();
        url += "?level=" + level;
        if (StringUtils.isNotEmpty(ownerId)) {
            url += "&ownerId=" + ownerId;
        }
        return get("getDataReport", url, DataReport.class);
    }
}
