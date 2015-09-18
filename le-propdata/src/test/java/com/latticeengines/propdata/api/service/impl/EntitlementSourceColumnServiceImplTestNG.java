package com.latticeengines.propdata.api.service.impl;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.service.EntitlementService;
import com.latticeengines.propdata.api.service.EntitlementSourceColumnService;

public class EntitlementSourceColumnServiceImplTestNG extends EntitlementServiceImplTestNGBase<
        EntitlementSourceColumnsPackages, EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap> {

    private static final String lookupId = "Experian_Source";
    private static final String columnName = "URL";

    @Autowired
    private EntitlementSourceColumnService entitlementService;

    @Override
    protected EntitlementService<EntitlementSourceColumnsPackages, EntitlementSourceColumnsPackageMap,
            EntitlementSourceColumnsContractPackageMap> getEntitlementService() {
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
        List<EntitlementSourceColumnsPackageMap> maps = entitlementService.getPackageSourceColumns(packageId);
        for (EntitlementSourceColumnsPackageMap map: maps) {
            if (map.getLookup_ID().equals(lookupId) && map.getColumnName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void addContentToPackage(Long packageId) {
        entitlementService.addColumnToPackage(packageId, lookupId, columnName);
    }

    @Override
    protected void removeContentFromPackage(Long packageId) {
        entitlementService.removeColumnFromPackage(packageId, lookupId, columnName);
    }

}
