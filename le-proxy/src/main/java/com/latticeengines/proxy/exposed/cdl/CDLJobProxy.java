package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import com.latticeengines.proxy.exposed.ProxyInterface;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlJobProxy")
public class CDLJobProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected CDLJobProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId createConsolidateJob(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeedjob/createconsolidatejob",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("createConsolidateJob", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ApplicationId.fromString(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId createProfileJob(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeedjob/createprofilejob",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("createprofileJob", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ApplicationId.fromString(appIdStr);
    }
}
