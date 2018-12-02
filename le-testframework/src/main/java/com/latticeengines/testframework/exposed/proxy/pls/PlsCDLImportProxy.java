package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.ResponseDocument;

@Service("plsCDLImportProxy")
public class PlsCDLImportProxy extends PlsRestApiProxyBase {

    public PlsCDLImportProxy() {
        super("pls/cdl/import");
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
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ApplicationId.fromString(appIdStr);
    }

}
