package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;

@Component("plsCDLS3TemplateImportProxy")
public class PlsCDLS3TemplateImportProxy extends PlsRestApiProxyBase {

    public PlsCDLS3TemplateImportProxy() {
        super("pls/cdl/s3/template/import");
    }

    public void doS3Import(String templateFileName, String source, boolean importData,
            S3ImportTemplateDisplay templateDisplay) {
        String url = constructUrl(
                "?feedType={feedType}&importData={importData}&source={source}&templateFileName={templateFileName}",
                templateDisplay.getFeedType(), importData, source, templateFileName);
        post("Do one-off s3 import", url, templateDisplay, Map.class);
    }

}
