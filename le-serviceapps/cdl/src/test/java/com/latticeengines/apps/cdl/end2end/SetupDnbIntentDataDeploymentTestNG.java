package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class SetupDnbIntentDataDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SetupDnbIntentDataDeploymentTestNG.class);
    private static final String DNBINTENT_SYSTEM = "Default_DnbIntent_System";

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "end2end")
    protected void test() {
        boolean created = cdlProxy.createDefaultDnbIntentDataTemplate(mainCustomerSpace);
        Assert.assertTrue(created, "Failed to create DnbIntentData template");
        String templateFeedType = EntityTypeUtils.generateFullFeedType(DNBINTENT_SYSTEM, EntityType.CustomIntent);
        S3ImportSystem importSystem = cdlProxy.getS3ImportSystem(mainCustomerSpace, DNBINTENT_SYSTEM);
        Assert.assertNotNull(importSystem,
                String.format("Should exist a DnbIntentData system. systemName=%s", DNBINTENT_SYSTEM));
        // verification
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", templateFeedType);
        Assert.assertNotNull(dataFeedTask);
        Table template = dataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.ModelName));
        Assert.assertNotNull(template.getAttribute(InterfaceName.LastModifiedDate));
        importOnlyDataFromS3(BusinessEntity.ActivityStream, "DnbIntentData_1ec2c252-f36d-4054-aaf4-9d6379006244.csv", dataFeedTask);
    }
}
