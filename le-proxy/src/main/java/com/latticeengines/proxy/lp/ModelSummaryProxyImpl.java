package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;


@Component
public class ModelSummaryProxyImpl extends MicroserviceRestApiProxy implements ModelSummaryProxy {

    protected ModelSummaryProxyImpl() {
        super("lp");
    }

    @Override
    public void setDownloadFlag(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadflag",
                shortenCustomerSpace(customerSpace));
        post("set model summary download flag", url, null);
    }

}
