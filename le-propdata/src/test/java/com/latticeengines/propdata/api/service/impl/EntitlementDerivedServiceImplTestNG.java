package com.latticeengines.propdata.api.service.impl;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.service.EntitlementDerivedService;
import com.latticeengines.propdata.api.service.EntitlementService;

public class EntitlementDerivedServiceImplTestNG extends EntitlementServiceImplTestNGBase<EntitlementPackages,
        EntitlementColumnMap, EntitlementContractPackageMap> {

    private static final String extensionName = "BusinessIndustry";
    private static final String sourceTableName = "Alexa_Source";

    @Autowired
    private EntitlementDerivedService entitlementService;

    @Override
    protected EntitlementService<EntitlementPackages, EntitlementColumnMap,
            EntitlementContractPackageMap> getEntitlementService() {
        return entitlementService;
    }

    @BeforeClass(groups = "api.functional")
    public void setup() {
        packageName = testEnv + packageName + "Derived";
        contractId = testEnv + contractId + "Derived";
        super.setup();
    }

    @Override
    protected boolean packageHasContent(Long packageId) {
        List<DataColumnMap> columns = entitlementService.getDerivedColumns(packageId);
        for (DataColumnMap column: columns) {
            if (column.getSourceTableName().equals(sourceTableName) &&
                    column.getExtension_Name().equals(extensionName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void addContentToPackage(Long packageId) {
        entitlementService.addDerivedColumnToPackage(packageId, extensionName, sourceTableName);
    }

    @Override
    protected void removeContentFromPackage(Long packageId) {
        entitlementService.removeDerivedColumnFromPackage(packageId, extensionName, sourceTableName);
    }

}
