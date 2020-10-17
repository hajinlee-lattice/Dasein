package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportRequest;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ProfileReportProxy;

@Component("profileReportProxy")
public class ProfileReportProxyImpl extends MicroserviceRestApiProxy implements ProfileReportProxy {

    protected ProfileReportProxyImpl() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    @Override
    public ApplicationId generateProfileReport(String customerSpace, AtlasProfileReportRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/profile-report", //
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> resp = post("submit profile report job", url, request, ResponseDocument.class);
        String appIdStr = resp.getResult();
        return ApplicationIdUtils.toApplicationIdObj(appIdStr);
    }

    @Override
    public AtlasProfileReportStatus getProfileReportStatus(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/profile-report/status", //
                shortenCustomerSpace(customerSpace));
        return get("get profile report status", url, AtlasProfileReportStatus.class);
    }

}
