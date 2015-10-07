package com.latticeengines.propdata.api.service.impl;


import java.util.List;

import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.HasPackageName;
import com.latticeengines.propdata.api.service.EntitlementService;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

@SuppressWarnings("hiding")
public abstract class EntitlementServiceImplTestNGBase<Package extends HasPid & HasPackageName,
        PackageContentMap, PackageContractMap> extends PropDataApiFunctionalTestNGBase {
    protected String packageName = "Test";
    protected String packageDescription = "A testing package";
    protected String contractId = "PropDataTest";

    private EntitlementService<Package, PackageContentMap, PackageContractMap> entitlementService;

    abstract protected Logger getLogger();

    @BeforeClass(groups = "api.functional")
    public void setup() {
        entitlementService = getEntitlementService();
    }

    @AfterClass(groups = "api.functional")
    public void teardown() {
        removePackageByName(packageName);
    }

    @BeforeMethod(groups = "api.functional")
    public void beforeMethod() {
        teardown();
    }

    @Test(groups = "api.functional", enabled = false)
    public void testCreatePackage() {
        getLogger().info("Testing package creation ... ");
        Assert.assertNull(entitlementService.findEntitilementPackageByName(packageName),
                "Test package should not exist before creation.");

        entitlementService.createDerivedPackage(packageName, packageDescription, false);

        Assert.assertNotNull(entitlementService.findEntitilementPackageByName(packageName),
                "Test package should exist after creation.");
    }

    @Test(groups = "api.functional")
    public void testGrantPackageToCustomer() {
        getLogger().info("Testing grant package to customer ... ");

        Long packageId = entitlementService.createDerivedPackage(packageName, packageDescription, false).getPid();
        Assert.assertFalse(customerHasPackage(contractId, packageName));

        entitlementService.grantEntitlementPackageToCustomer(packageId, contractId);
        Assert.assertTrue(customerHasPackage(contractId, packageName));

        entitlementService.revokeEntitlementPackageToCustomer(packageId, contractId);
        Assert.assertFalse(customerHasPackage(contractId, packageName));
    }

    @Test(groups = "api.functional")
    public void testAddContentToPackage() {
        getLogger().info("Testing add content to customer ... ");

        Long packageId = entitlementService.createDerivedPackage(packageName, packageDescription, false).getPid();
        Assert.assertFalse(packageHasContent(packageId));

        addContentToPackage(packageId);
        Assert.assertTrue(packageHasContent(packageId));

        removeContentFromPackage(packageId);
        Assert.assertFalse(packageHasContent(packageId));
    }

    @Test(groups = "api.functional")
    public void testDeletionCascade() {
        getLogger().info("Testing cascading on deletion ... ");

        Assert.assertNull(entitlementService.findEntitilementPackageByName(packageName));
        Assert.assertFalse(customerHasPackage(contractId, packageName));

        Long packageId = entitlementService.createDerivedPackage(packageName, packageDescription, false).getPid();
        Assert.assertNotNull(entitlementService.findEntitilementPackageByName(packageName));
        Assert.assertFalse(packageHasContent(packageId));

        entitlementService.grantEntitlementPackageToCustomer(packageId, contractId);
        Assert.assertTrue(customerHasPackage(contractId, packageName));

        addContentToPackage(packageId);
        Assert.assertTrue(packageHasContent(packageId));

        removePackageByName(packageName);
        Assert.assertNull(entitlementService.findEntitilementPackageByName(packageName));
        Assert.assertFalse(customerHasPackage(contractId, packageName));
        Assert.assertFalse(packageHasContent(packageId));
    }

    private void removePackageByName(String packageName) {
        Package pkg = entitlementService.findEntitilementPackageByName(packageName);
        if (pkg != null) {
            entitlementService.removeEntitlementPackage(pkg.getPid());
        }
    }

    private boolean customerHasPackage(String contractId, String packageName) {
        List<Package> pkgs = entitlementService.getEntitlementPackagesForCustomer(contractId);
        for (Package pkg: pkgs) {
            if (pkg.getPackageName().equals(packageName)) {
                return true;
            }
        }
        return false;
    }

    abstract EntitlementService<Package, PackageContentMap, PackageContractMap> getEntitlementService();

    abstract boolean packageHasContent(Long packageId);
    abstract void addContentToPackage(Long packageId);
    abstract void removeContentFromPackage(Long packageId);
}
