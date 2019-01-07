package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;

public class CSVFileImportWithoutIdDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportWithoutIdDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.IMPORT_WITHOUT_ID.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testImport() {
        prepareBaseData(ENTITY_ACCOUNT);
        getDataFeedTask(ENTITY_ACCOUNT);

        Table accountTemplate = accountDataFeedTask.getImportTemplate();
        Assert.assertNull(accountTemplate.getAttribute(InterfaceName.AccountId));
    }

}
