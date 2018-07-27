package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.ModelSummary;
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

    @Override
    public ModelSummary getModelSummaryById(String customerSpace, String modelSummaryId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/{modelSummaryId}",
                shortenCustomerSpace(customerSpace), modelSummaryId);
        return get("set model summary download flag", url, ModelSummary.class);
    }
}
