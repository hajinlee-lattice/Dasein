package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
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

public class processMarketingActivityDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(processMarketingActivityDeploymentTestNG.class);
    private static final String MARKETO_SYSTEM = "Default_Marketo_System";
    private static final String ELOQUA_SYSTEM = "Default_Eloqua_System";

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
    protected void test() throws Exception {
        setupMarketingActivityTemplatesAndVerify(MARKETO_SYSTEM, S3ImportSystem.SystemType.Marketo.name(),
                "marketo_marketing_activity_1ec2c252-f36d-4054-aaf4-9d6379006244.csv");
        setupMarketingActivityTemplatesAndVerify(ELOQUA_SYSTEM, S3ImportSystem.SystemType.Eloqua.name(),
                "eloqua_marketing_activity_1ec2c252-f36d-4054-aaf4-9d6379006244.csv");
    }

    private void createMaerketingActivitySystem(String systemName) {
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        system.setName(systemName);
        system.setDisplayName(systemName);
        system.setSystemType(S3ImportSystem.SystemType.Other);
        system.setPriority(2);
        // dummy id, maybe just use the customer account id
        system.setContactSystemId(String.format("user_%s_dlugenoz_ContactId", systemName));
        system.setMapToLatticeContact(true);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
    }

    private void setupMarketingActivityTemplatesAndVerify(String systemName, String systemType, String fileName) throws Exception {
        createMaerketingActivitySystem(systemName);
        Thread.sleep(2000L);
        Assert.assertTrue(createS3Folder(systemName, Arrays.asList(EntityType.MarketingActivity,
                EntityType.MarketingActivityType)));

        // setup templates
        boolean created = cdlProxy.createDefaultMarketingTemplate(mainCustomerSpace, systemName, systemType);
        Assert.assertTrue(created,
                String.format("Failed to create MarketingActivity template in system %s", systemName));
        String templateFeedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.MarketingActivity);
        String catalogFeedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.MarketingActivityType);

        S3ImportSystem importSystem = cdlProxy.getS3ImportSystem(mainCustomerSpace, systemName);
        Assert.assertNotNull(importSystem,
                String.format("Should exist a marketingActivity system. systemName=%s", systemName));
        // verification
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", templateFeedType);
        Assert.assertNotNull(dataFeedTask);
        Table template = dataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.ActivityDate));
        Assert.assertNotNull(template.getAttribute(InterfaceName.ActivityType));
        Assert.assertNotNull(template.getAttribute(importSystem.getContactSystemId()));

        //verification Catalog
        DataFeedTask catalogTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace, "File", catalogFeedType);
        Assert.assertNotNull(catalogTask);
        template = catalogTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.ActivityType));
        Assert.assertNotNull(template.getAttribute(InterfaceName.Name));

        // import marketingActivity & activityType data
        importOnlyDataFromS3(BusinessEntity.ActivityStream, fileName, dataFeedTask);
        importOnlyDataFromS3(BusinessEntity.Catalog, "marketing_activity_type.csv", catalogTask);
    }

}
