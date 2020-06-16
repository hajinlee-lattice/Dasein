package com.latticeengines.proxy.exposed.dcp;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dataReportProxy")
public class DataReportProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected DataReportProxy() {
        super("dcp");
    }

    public DataReport getDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get Data Report", url, DataReport.class);
    }

    public DataReport.BasicStats getDataReportBasicStats(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/basicstats?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get Data Report", url, DataReport.BasicStats.class);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport dataReport) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, dataReport);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.BasicStats basicStats) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/basicstats?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, basicStats);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.InputPresenceReport inputPresenceReport) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/inputpresencereport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, inputPresenceReport);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.GeoDistributionReport geoDistributionReport) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/geodistributionreport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, geoDistributionReport);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.MatchToDUNSReport matchToDUNSReport) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/matchtodunsreport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, matchToDUNSReport);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId,
                                 DataReport.DuplicationReport duplicationReport) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/duplicationreport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        post("Update Data Report", url, duplicationReport);
    }

    private String getUrl(String customerSpace, DataReportRecord.Level level, String ownerId, String baseUrl) {
        String url;
        if (StringUtils.isNotEmpty(ownerId)) {
            baseUrl += "&ownerId={ownerId}";
            url = constructUrl(baseUrl, customerSpace, level, ownerId);
        } else {
            url = constructUrl(baseUrl, customerSpace, level);
        }
        return url;
    }
}
