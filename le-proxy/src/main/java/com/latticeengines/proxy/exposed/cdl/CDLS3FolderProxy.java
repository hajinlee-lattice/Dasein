package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlS3FolderProxy")
public class CDLS3FolderProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected CDLS3FolderProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public String moveToSucceed(String customerSpace, String key) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/s3import/succeed", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("", url, key, ResponseDocument.class);
        if (responseDoc.isSuccess()) {
            return responseDoc.getResult();
        } else {
            return StringUtils.EMPTY;
        }
    }

    @SuppressWarnings("unchecked")
    public String moveToFailed(String customerSpace, String key) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/s3import/failed", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("", url, key, ResponseDocument.class);
        if (responseDoc.isSuccess()) {
            return responseDoc.getResult();
        } else {
            return StringUtils.EMPTY;
        }
    }
}
