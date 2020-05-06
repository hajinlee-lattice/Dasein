package com.latticeengines.testframework.exposed.proxy.pls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.latticeengines.domain.exposed.pls.SourceFile;

@Service("importFileProxy")
public class ImportFileProxy extends PlsRestApiProxyBase {

    private static final Logger log = LoggerFactory.getLogger(ImportFileProxy.class);

    public ImportFileProxy() {
        super("pls/importfile");
    }

    public SourceFile uploadFile(String name, Resource fileResource) {
        String urlPattern = "?name={name}";
        String url = constructUrl(urlPattern, name);
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("file", fileResource);
        return postMultiPart("upload file", url, parts, SourceFile.class);
    }
}
