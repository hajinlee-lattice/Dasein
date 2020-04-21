package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
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
    public Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        return post("create upload", url, uploadConfig, Upload.class);
    }

    @Override
    public List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        if (status != null) {
            url = url + "?status=" + status;
        }
        return JsonUtils.convertList(get("get uploads", url, List.class), Upload.class);
    }

    @Override
    public Upload getUpload(String customerSpace, Long uploadPid) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{pid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid);
        return get("Get Upload by Pid", url, Upload.class);
    }

    @Override
    public void registerMatchResult(String customerSpace, long uploadPid, String tableName) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadPid}/matchResult/{tableName}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid, tableName);
        put("register Upload match result", url);
    }

    @Override
    public void registerMatchCandidates(String customerSpace, long uploadPid, String tableName) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadPid}/matchCandidates/{tableName}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid, tableName);
        put("register Upload match candidates", url);
    }

    @Override
    public void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadPid}/config";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid);
        log.info("Update config for Upload " + uploadPid);
        put("update Upload config", url, uploadConfig);
    }

    @Override
    public void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/update/{uploadPid}/status/{status}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid, status);
        log.info("Update status for Upload " + uploadPid + " to " + status);
        put("update Upload status", url);
    }

    @Override
    public void updateStatsContent(String customerSpace, long uploadPid, long statsPid, UploadStats uploadStats) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{pid}/stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid, statsPid);
        log.info("Update stats for Upload " + uploadPid + " to " + JsonUtils.serialize(uploadStats));
        put("update Upload status", url, uploadStats);
    }

    @Override
    public void setLatestStats(String customerSpace, long uploadPid, long statsPid) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/{pid}/latest-stats/{statsPid}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), uploadPid, statsPid);
        log.info("Update latest stats for Upload " + uploadPid + " to " + statsPid);
        put("set Upload latest statistics", url);
    }

    @Override
    public void sendUploadCompletedEmail(String customerSpace, UploadEmailInfo uploadEmailInfo) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/email/completed";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));
        log.info("Send upload completed mail for Upload " + uploadEmailInfo.getUploadId());
        put("Send Upload Completed Mail", url, uploadEmailInfo);
    }

}
