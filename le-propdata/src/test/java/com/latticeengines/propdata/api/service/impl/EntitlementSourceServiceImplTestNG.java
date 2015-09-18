package com.latticeengines.propdata.api.service.impl;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.service.EntitlementService;
import com.latticeengines.propdata.api.service.EntitlementSourceService;

public class EntitlementSourceServiceImplTestNG extends EntitlementServiceImplTestNGBase<EntitlementSourcePackages,
        EntitlementSourcePackageMap, EntitlementSourceContractPackageMap> {

    private static final String lookupId = "Alexa_Source";

    @Autowired
    private EntitlementSourceService entitlementService;

    @Override
    protected EntitlementService<EntitlementSourcePackages,
            EntitlementSourcePackageMap, EntitlementSourceContractPackageMap> getEntitlementService() {
        return entitlementService;
    }

    @BeforeClass(groups = "api.functional")
    public void setup() {
        packageName = testEnv + packageName + "Source";
        contractId = testEnv + contractId + "Source";
        super.setup();
    }

    @Override
    protected boolean packageHasContent(Long packageId) {
        List<EntitlementSourcePackageMap> maps = entitlementService.getSourceEntitlementContents(packageId);
        for (EntitlementSourcePackageMap map: maps) {
            if (map.getLookup_ID().equals(lookupId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void addContentToPackage(Long packageId) {
        entitlementService.addSourceToPackage(packageId, lookupId);
    }

    @Override
    protected void removeContentFromPackage(Long packageId) {
        entitlementService.removeSourceFromPackage(packageId, lookupId);
    }

}
