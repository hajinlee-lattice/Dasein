package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;

@Component("plsCDLS3ImportProxy")
public class PlsCDLS3ImportProxy extends PlsRestApiProxyBase {
    private static final String ROOT_PATH = "pls/cdl/s3import";

    public PlsCDLS3ImportProxy() {
        super(ROOT_PATH);
    }

    public PlsCDLS3ImportProxy(String hostport) {
        super(hostport, ROOT_PATH);
    }

    public List<S3ImportTemplateDisplay> getS3ImportTemplateEntries() {
        String url = constructUrl("/template");
        List<?> rawList = get("get all templates", url, List.class);
        return JsonUtils.convertList(rawList, S3ImportTemplateDisplay.class);
    }
}
