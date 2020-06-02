package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;

@Component("testUploadProxy")
public class TestUploadProxy extends PlsRestApiProxyBase {

    public TestUploadProxy() {
        super("pls/uploads");
    }

    public UploadDetails startImport(DCPImportRequest importRequest) {
        String urlPattern = "/startimport";
        String url = constructUrl(urlPattern);
        return post("start import", url, importRequest, UploadDetails.class);
    }

    public List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status) {
        String baseUrl = "/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, sourceId);
        if (status != null) {
            url = url + "?status=" + status;
        }
        return JsonUtils.convertList(get("get uploads by sourceId", url, List.class), UploadDetails.class);
    }

    public UploadDetails getUpload(String uploadId) {
        String urlPattern = "/uploadId/{uploadId}";
        String url = constructUrl(urlPattern, uploadId);
        return get("get upload by uploadId", url, UploadDetails.class);
    }

    public String getToken(String uploadId) {
        String urlPattern = "/uploadId/{uploadId}/token";
        String url = constructUrl(urlPattern, uploadId);
        return get("Generate a token for downloading zip file of the upload results", url, String.class);
    }
}
