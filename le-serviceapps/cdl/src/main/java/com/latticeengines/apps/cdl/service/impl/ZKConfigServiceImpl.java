package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

@Service("zKConfigService")
public class ZKConfigServiceImpl implements ZKConfigService {

    private static final Logger log = LoggerFactory.getLogger(ZKConfigServiceImpl.class);

    @Inject
    private BatonService batonService;

    @Override
    public String getFakeCurrentDate(CustomerSpace customerSpace) {
        try {
            String fakeCurrentDate = null;
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    CDLComponent.componentName);
            Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(fakeCurrentDatePath)) {
                fakeCurrentDate = camille.get(fakeCurrentDatePath).getData();
            }
            return fakeCurrentDate;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get FakeCurrentDate from ZK for " + customerSpace.getTenantId(), e);
        }
    }

    @Override
    public int getInvokeTime(CustomerSpace customerSpace) {
        try {
            int invokeTime = 0;
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    CDLComponent.componentName);
            Path invokeTimePath = cdlPath.append("InvokeTime");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(invokeTimePath)) {
                invokeTime = Integer.parseInt(camille.get(invokeTimePath).getData());
            }
            return invokeTime;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get InvokeTime from ZK for " + customerSpace.getTenantId(), e);
        }
    }

    @Override
    public boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace) {
        try {
            return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
        } catch (Exception e) {
            log.warn("Failed to tell if InternalEnrichment is enabled in ZK for " + customerSpace.getTenantId() + ": " + e.getMessage());
            return false;
        }
    }

}
