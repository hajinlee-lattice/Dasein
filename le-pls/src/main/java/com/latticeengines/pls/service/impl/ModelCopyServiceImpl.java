package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.proxy.exposed.lp.ModelCopyProxy;

@Component("modelCopyService")
public class ModelCopyServiceImpl implements ModelCopyService {

    @Inject
    private ModelCopyProxy modelCopyProxy;

    @Override
    public boolean copyModel(String sourceTenantId, String targetTenantId, String modelId) {
        modelCopyProxy.copyModel(sourceTenantId, targetTenantId, modelId);
        return true;
    }

    @Override
    public boolean copyModel(String targetTenantId, String modelId) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        return copyModel(space.toString(), targetTenantId, modelId);
    }
}
