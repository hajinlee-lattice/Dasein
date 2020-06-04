package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("uploadProxy")
public class UploadProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(UploadProxy.class);

    protected UploadProxy() {
        super("dcp");
    }

    public UploadProxy(String hostPort) {
        super(hostPort, "dcp");
    }

    public UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        return post("create upload", url, uploadConfig, UploadDetails.class);
    }

    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        if (status != null) {
            url = url + "?status=" + status;
        }
        return JsonUtils.convertList(get("get uploads", url, List.class), UploadDetails.class);
    }

    public UploadDetails getUploadByUploadId(String customerSpace, String uploadId) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/uploadId/{uploadId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId);
        return get("Get Upload by UploadId", url, UploadDetails.class);
    }

    public void registerMatchResult(String customerSpace, String uploadId, String tableName) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/matchResult/{tableName}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, tableName);
        put("register Upload match result", url);
    }

    public void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/config";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId);
        log.info("Update config for Upload " + uploadId);
        put("update Upload config", url, uploadConfig);
    }

    public void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status, UploadDiagnostics uploadDiagnostics) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/status/{status}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, status);
        log.info("Update status for Upload " + uploadId + " to " + status);
        if(uploadDiagnostics != null) {
            put("update Upload status", url, uploadDiagnostics);
        } else {
            put("update Upload status", url);
        }

    }

    public void updateStatsContent(String customerSpace, String uploadId, long statsPid, UploadStats uploadStats) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{uploadId}/stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, statsPid);
        log.info("Update stats for Upload " + uploadId + " to " + JsonUtils.serialize(uploadStats));
        put("update Upload status", url, uploadStats);
    }

    public void setLatestStats(String customerSpace, String uploadId, long statsPid) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{uploadId}/latest-stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, statsPid);
        log.info("Update latest stats for Upload " + uploadId + " to " + statsPid);
        put("set Upload latest statistics", url);
    }

    public ApplicationId startImport(String customerSpace, DCPImportRequest request) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/startimport";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));
        String appIdStr = post("Start DCP import", url, request, String.class);
        return ApplicationId.fromString(appIdStr);
    }
}
