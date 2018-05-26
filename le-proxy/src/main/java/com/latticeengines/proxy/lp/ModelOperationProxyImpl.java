package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.lp.ReplaceModelRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelOperationProxy;


@Component
public class ModelOperationProxyImpl extends MicroserviceRestApiProxy implements ModelOperationProxy {

    protected ModelOperationProxyImpl() {
        super("lp");
    }

    @Override
    public Boolean cleanUpModel(String modelId) {
        String url = constructUrl("/modeloperation/cleanup/modelid/{modelId}", modelId);
        delete("cleanup model", url);
        return true;
    }


    @Override
    public Boolean replaceModel(String sourceTenantId, String sourceModelId, String targetTenantId, String targetModelId) {
        String url = constructUrl("/modeloperation/replace");
        ReplaceModelRequest request = new ReplaceModelRequest();
        request.setSourceTenant(shortenCustomerSpace(sourceTenantId));
        request.setSourceModelGuid(sourceModelId);
        request.setTargetTenant(shortenCustomerSpace(targetTenantId));
        request.setTargetModelGuid(targetModelId);
        post("replace model", url, request);
        return true;
    }

}
