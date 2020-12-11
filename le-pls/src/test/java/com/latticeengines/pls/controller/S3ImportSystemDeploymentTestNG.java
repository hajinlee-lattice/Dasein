package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.PlsCDLS3ImportProxy;

public class S3ImportSystemDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private PlsCDLS3ImportProxy plsCDLS3ImportProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(plsCDLS3ImportProxy);
    }

    @Test(groups = "deployment")
    public void testAutomaticCreate() {

        // no system has account system Id
        List<S3ImportSystem> systems = plsCDLS3ImportProxy.getS3ImportSystem(true, false);
        Assert.assertTrue(CollectionUtils.isEmpty(systems));

        // no system has contact system id
        systems = plsCDLS3ImportProxy.getS3ImportSystem(false, true);
        Assert.assertTrue(CollectionUtils.isEmpty(systems));

        // create salesforce system, verify generate account system id
        Map<String, UIAction> result = plsCDLS3ImportProxy.createS3ImportSystem("salesforce1",
                S3ImportSystem.SystemType.Salesforce, true);
        Assert.assertTrue(MapUtils.isNotEmpty(result));

        systems = plsCDLS3ImportProxy.getS3ImportSystem(true, false);
        Assert.assertTrue(CollectionUtils.isNotEmpty(systems));
        Assert.assertEquals(systems.size(), 1);

        // create pardon system , verify generate contact system id
        result = plsCDLS3ImportProxy.createS3ImportSystem("pardon1", S3ImportSystem.SystemType.Pardon, true);
        Assert.assertTrue(MapUtils.isNotEmpty(result));
        Assert.assertEquals(systems.size(), 1);

        systems = plsCDLS3ImportProxy.getS3ImportSystem(false, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(systems));
        Assert.assertEquals(systems.size(), 1);
    }

}
