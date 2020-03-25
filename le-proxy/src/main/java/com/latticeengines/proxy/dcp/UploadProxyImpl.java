package com.latticeengines.proxy.dcp;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Component("uploadProxy")
public class UploadProxyImpl extends MicroserviceRestApiProxy implements UploadProxy {

    public UploadProxyImpl() {
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
    public void updateConfig(String customerSpace, Long uploadId, UploadConfig uploadConfig) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/uploadId/{uploadId}/config";
        String url = constructUrl(baseUrl, customerSpace, uploadId);
        put("update config", url, uploadConfig, Void.class);

    }

    @Override
    public void updateStatus(String customerSpace, Long uploadId, Upload.Status status) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/uploadId/{uploadId}/status/{status}";
        String url = constructUrl(baseUrl, customerSpace, uploadId, status);
        put("update status", url);

    }
}
