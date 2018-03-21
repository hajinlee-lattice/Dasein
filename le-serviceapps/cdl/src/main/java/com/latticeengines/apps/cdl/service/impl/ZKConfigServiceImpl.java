package com.latticeengines.apps.cdl.service.impl;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

@Service("zKConfigService")
public class ZKConfigServiceImpl implements ZKConfigService {


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

}
