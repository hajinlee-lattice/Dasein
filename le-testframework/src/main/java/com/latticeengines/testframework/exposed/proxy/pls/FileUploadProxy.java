package com.latticeengines.testframework.exposed.proxy.pls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.latticeengines.domain.exposed.dcp.SourceFileInfo;

@Component("fileUploadProxy")
public class FileUploadProxy extends PlsRestApiProxyBase {

    private static final Logger log = LoggerFactory.getLogger(FileUploadProxy.class);

    public FileUploadProxy() {
        super("pls/fileuploads");
    }

    public SourceFileInfo uploadFile(String name, Resource fileResource) {
        String urlPattern = "/{name}";
        String url = constructUrl(urlPattern, name);
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("file", fileResource);
        return postMultiPart("upload file", url, parts, SourceFileInfo.class);
    }
}
