package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
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
}
