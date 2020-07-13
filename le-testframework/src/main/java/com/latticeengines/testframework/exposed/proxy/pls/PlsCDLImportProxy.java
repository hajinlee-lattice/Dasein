package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;

@Service("plsCDLImportProxy")
public class PlsCDLImportProxy extends PlsRestApiProxyBase {
    private static final String ROOT_PATH = "pls/jobs";

    public PlsCDLImportProxy() {
        super(ROOT_PATH);
    }

    public PlsCDLImportProxy(String hostport) {
        super(hostport, ROOT_PATH);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId startImportCSV(String templateFileName, String dataFileName, String source, String entity,
                                        String feedType) {
        String urlPattern = "/csv?templateFileName={templateFileName}&dataFileName={dataFileName}&source={source}" +
                "&entity={entity}&feedType={feedType}";
        String url = constructUrl(urlPattern, templateFileName, dataFileName, source, entity, feedType);
        ResponseDocument<String> responseDoc = post("start import csv",  url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
    }

}
