package com.latticeengines.proxy.exposed.dcp;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.dataReport.DataReportRollupStatus;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dataReportProxy")
public class DataReportProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected DataReportProxy() {
        super("dcp");
    }

    public DataReportProxy(String hostPort) {
        super(hostPort, "dcp");
    }

    public void registerDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId,
                                  DunsCountCache cache) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/dunscount?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        put("Register duns count", url, cache);
    }

    public DunsCountCache getDunsCount(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/dunscount?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get duns count", url, DunsCountCache.class);
    }

    public Set<String> getChildrenIds(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/childrenids?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return getSet("Get children's owner ids", url, String.class);
    }

    public DataReport getDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get Data Report", url, DataReport.class);
    }

    public DataReport getReadyForRollupDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/readyforrollup?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get Ready For Rollup Data Report", url, DataReport.class);
    }

    public DataReport.BasicStats getDataReportBasicStats(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/basicstats?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        return get("Get Data Report", url, DataReport.BasicStats.class);
    }

    public void updateDataReport(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/readyforrollup?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        put("Update Data Report", url);
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

    public ApplicationId rollupDataReport(String customerSpace, DCPReportRequest request) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/rollup";
        String url = constructUrl(baseUrl, customerSpace);
        String appId =  post("rollup data report", url, request, String.class);
        return ApplicationIdUtils.toApplicationIdObj(appId);
    }

    public void updateRollupStatus(String customerSpace, DataReportRollupStatus rollupStatus) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/rollup_status";
        String url = constructUrl(baseUrl, customerSpace);
        put("Update ", url, rollupStatus);
    }

    public void copyDataReportToParent(String customerSpace, DataReportRecord.Level level, String ownerId) {
        String baseUrl = "/customerspaces/{customerSpace}/datareport/copytoparent?level={level}";
        String url = getUrl(customerSpace, level, ownerId, baseUrl);
        put("Copy data report to parent", url);
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
