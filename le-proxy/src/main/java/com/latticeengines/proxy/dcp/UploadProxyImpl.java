package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Component("uploadProxy")
public class UploadProxyImpl extends MicroserviceRestApiProxy implements UploadProxy {

    private static final Logger log = LoggerFactory.getLogger(UploadProxyImpl.class);

    protected UploadProxyImpl() {
        super("dcp");
    }

    @Override
    public UploadDetails createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        return post("create upload", url, uploadConfig, UploadDetails.class);
    }

    @Override
    public List<UploadDetails> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        if (status != null) {
            url = url + "?status=" + status;
        }
        return JsonUtils.convertList(get("get uploads", url, List.class), UploadDetails.class);
    }

    @Override
    public UploadDetails getUploadByUploadId(String customerSpace, String uploadId) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/uploadId/{uploadId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId);
        return get("Get Upload by UploadId", url, UploadDetails.class);
    }

    @Override
    public void registerMatchResult(String customerSpace, String uploadId, String tableName) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/matchResult/{tableName}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, tableName);
        put("register Upload match result", url);
    }

    @Override
    public void registerMatchCandidates(String customerSpace, String uploadId, String tableName) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/matchCandidates/{tableName}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, tableName);
        put("register Upload match candidates", url);
    }

    @Override
    public void updateUploadConfig(String customerSpace, String uploadId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/config";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId);
        log.info("Update config for Upload " + uploadId);
        put("update Upload config", url, uploadConfig);
    }

    @Override
    public void updateUploadStatus(String customerSpace, String uploadId, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadId}/status/{status}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, status);
        log.info("Update status for Upload " + uploadId + " to " + status);
        put("update Upload status", url);
    }

    @Override
    public void updateStatsContent(String customerSpace, String uploadId, long statsPid, UploadStats uploadStats) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{uploadId}/stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, statsPid);
        log.info("Update stats for Upload " + uploadId + " to " + JsonUtils.serialize(uploadStats));
        put("update Upload status", url, uploadStats);
    }

    @Override
    public void setLatestStats(String customerSpace, String uploadId, long statsPid) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{uploadId}/latest-stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadId, statsPid);
        log.info("Update latest stats for Upload " + uploadId + " to " + statsPid);
        put("set Upload latest statistics", url);
    }
}
