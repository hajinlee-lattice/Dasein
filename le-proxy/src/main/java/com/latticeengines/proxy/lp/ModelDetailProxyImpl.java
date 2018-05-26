package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.ModelDetail;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelDetailProxy;

@Component
public class ModelDetailProxyImpl extends MicroserviceRestApiProxy implements ModelDetailProxy {

    protected ModelDetailProxyImpl() {
        super("lp");
    }

    @Override
    public ModelDetail getModelDetail(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modeldetail/modelid/{modelId}",
                shortenCustomerSpace(customerSpace), modelId);
        return get("get model detail", url, ModelDetail.class);
    }

}
