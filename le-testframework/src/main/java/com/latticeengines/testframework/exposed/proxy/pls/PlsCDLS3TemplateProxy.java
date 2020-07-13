package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;

@Component("plsCDLS3TemplateProxy")
public class PlsCDLS3TemplateProxy extends PlsRestApiProxyBase {
    private static final String ROOT_PATH = "pls/cdl/s3/template";

    public PlsCDLS3TemplateProxy() {
        super(ROOT_PATH);
    }

    public PlsCDLS3TemplateProxy(String hostport) {
        super(hostport, ROOT_PATH);
    }

    public void createS3Template(String templateFileName, String source, boolean importData,
            S3ImportTemplateDisplay templateDisplay) {
        String url = constructUrl("?importData={importData}&source={source}&templateFileName={templateFileName}", importData, source, templateFileName);
        post("create s3 template", url, templateDisplay, Map.class);
    }
}
