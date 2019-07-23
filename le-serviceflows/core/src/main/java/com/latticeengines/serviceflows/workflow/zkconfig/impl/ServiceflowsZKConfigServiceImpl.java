package com.latticeengines.serviceflows.workflow.zkconfig.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.serviceflows.workflow.zkconfig.ServiceflowsZKConfigService;

@Service("serviceflowsZKConfigService")
public class ServiceflowsZKConfigServiceImpl implements ServiceflowsZKConfigService  {

    @Inject
    private BatonService batonService;

    @Override
    public boolean isEnabledForInternalEnrichment(CustomerSpace customerSpace) {
        return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
    }

}
