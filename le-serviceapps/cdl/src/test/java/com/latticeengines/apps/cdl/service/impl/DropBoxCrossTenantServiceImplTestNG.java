package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DropBoxCrossTenantService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

public class DropBoxCrossTenantServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DropBoxCrossTenantServiceImplTestNG.class);

    @Inject
    private DropBoxCrossTenantService dropBoxCrossTenantService;
    @Inject
    private S3Service s3Service;

    private String customerSpace;
    private Long tenantPid;

    @BeforeClass(groups = "functional")
    public void setup() {
        testBed.bootstrap(1);
        Tenant testTenant = testBed.getMainTestTenant();
        customerSpace = CustomerSpace.parse(testTenant.getId()).toString();
        tenantPid = testTenant.getPid();
    }

    @Test(groups = "functional")
    public void testCrudLatticeUser() {
        String prefix = testCreate();
        testDelete(prefix);
    }

    private String testCreate() {
        dropBoxCrossTenantService.create(customerSpace);
        String prefix = dropBoxCrossTenantService.getDropBoxPrefix(customerSpace);
        Assert.assertTrue(StringUtils.isNotBlank(prefix));
        Assert.assertTrue(s3Service.objectExist(dropBoxCrossTenantService.getDropBoxBucket(), prefix + "/"));
        Tenant owner = dropBoxCrossTenantService.getDropBoxOwner(dropBoxCrossTenantService.getDropBoxSummary(customerSpace).getDropBox());
        Assert.assertNotNull(owner);
        Assert.assertEquals(owner.getPid(), tenantPid);
        return prefix;
    }

    private void testDelete(String prefix) {
        dropBoxCrossTenantService.delete(customerSpace);
        Assert.assertFalse(StringUtils.isNotBlank(dropBoxCrossTenantService.getDropBoxPrefix(customerSpace)));
        Assert.assertFalse(s3Service.objectExist(dropBoxCrossTenantService.getDropBoxBucket(), prefix + "/"));
    }
}
