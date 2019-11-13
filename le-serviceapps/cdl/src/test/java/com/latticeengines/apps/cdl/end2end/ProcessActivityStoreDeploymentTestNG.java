package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.EntityTypeUtils.generateFullFeedType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class ProcessActivityStoreDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String WEBSITE_SYSTEM = "Default_Website_System";

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = { "end2end" })
    @Override
    public void setup() throws Exception {
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "end2end")
    private void test() throws Exception {
        // resume from process account checkpoint to make matching stream faster
        resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT);

        // setup webvisit template one by one for now, batch setup sometimes having
        // problem
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        SimpleTemplateMetadata webVisit = new SimpleTemplateMetadata();
        webVisit.setEntityType(EntityType.WebVisit);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(webVisit));
        SimpleTemplateMetadata ptn = new SimpleTemplateMetadata();
        ptn.setEntityType(EntityType.WebVisitPathPattern);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(ptn));
        SimpleTemplateMetadata sm = new SimpleTemplateMetadata();
        sm.setEntityType(EntityType.WebVisitSourceMedium);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(sm));

        // import visit/ptn/source files (20k streams, 4 accounts, 450 days)
        String webVisitCsvFile = "webVisit_1fe0719c-18ac-4348-b2bb-e6ee8fbc6882.csv";
        String ptnCsvFile = "webVisitPathPtn.csv";
        String smCsvFile = "webVisitSrcMedium.csv";
        importData(BusinessEntity.Catalog, smCsvFile,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitSourceMedium), false, false);
        Thread.sleep(2000);
        importData(BusinessEntity.ActivityStream, webVisitCsvFile,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisit), false, false);
        Thread.sleep(2000);
        importData(BusinessEntity.Catalog, ptnCsvFile,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitPathPattern), false, false);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());

        // run PA
        processAnalyzeSkipPublishToS3();
    }
}
