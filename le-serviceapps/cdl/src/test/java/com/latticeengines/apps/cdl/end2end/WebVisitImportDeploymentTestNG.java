package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.EntityTypeUtils.generateFullFeedType;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

public class WebVisitImportDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WebVisitImportDeploymentTestNG.class);

    private String customerSpace;

    @Value("${cdl.test.import.filename.webvisit}")
    private String webVisitCSV;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_MULTI_TEMPLATE_IMPORT.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        log.info("Setup Complete!");
    }

    @Test(groups = "end2end")
    public void runTest() {
        importData();
    }

    private void importData() {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        S3ImportSystem s3ImportSystem = new S3ImportSystem();
        String systemDisplayName = "Default_Website";
        String systemName = AvroUtils.getAvroFriendlyString(systemDisplayName);
        s3ImportSystem.setSystemType(S3ImportSystem.SystemType.Website);
        s3ImportSystem.setName(systemName);
        s3ImportSystem.setDisplayName(systemDisplayName);
        s3ImportSystem.setTenant(MultiTenantContext.getTenant());
        s3ImportSystemService.createS3ImportSystem(customerSpace, s3ImportSystem);
        importData(BusinessEntity.ActivityStream, webVisitCSV, generateFullFeedType("Default_Website", EntityType.WebVisit), false,
                false);
    }
}
