package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlProxy")
public class CDLProxy extends MicroserviceRestApiProxy {

    protected CDLProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId consolidate(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/consolidate", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("consolidate", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId profile(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/profile", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("profile", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public boolean reset(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/reset", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("kickoff reset", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return false;
        }
        String statusStr = responseDoc.getResult();
        return ("Success".equals(statusStr));
    }

}
