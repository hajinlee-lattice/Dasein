package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.EntityTypeUtils.generateFullFeedType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class ProcessActivityStoreDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String WEBSITE_SYSTEM = "Default_Website_System";
    private static final Instant CURRENT_PA_TIME = LocalDate.of(2017, 8, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    @Inject
    private ActionProxy actionProxy;

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
        resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT);

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        setupTemplates();
        mockCSVImport(BusinessEntity.ActivityStream, ADVANCED_MATCH_SUFFIX, 1,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisit));
        Thread.sleep(2000);
        // webVisitPathPtn.csv
        mockCSVImport(BusinessEntity.Catalog, ADVANCED_MATCH_SUFFIX, 2,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitPathPattern));
        Thread.sleep(2000);
        // webVisitSrcMedium.csv
        mockCSVImport(BusinessEntity.Catalog, ADVANCED_MATCH_SUFFIX, 3,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitSourceMedium));
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());

        // run PA with fake current time
        processAnalyzeSkipPublishToS3(CURRENT_PA_TIME.toEpochMilli());
    }

    @Test(groups = "end2end", dependsOnMethods = "test", enabled = false)
    private void testRematch() throws Exception {
        importSmallWebVisitFile();

        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setSkipPublishToS3(true);
        request.setSkipDynamoExport(true);
        request.setFullRematch(true);
        request.setSkipEntities(
                Sets.newHashSet(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Transaction));
        request.setCurrentPATimestamp(CURRENT_PA_TIME.toEpochMilli());
        processAnalyze(request);
    }

    @Test(groups = "end2end", dependsOnMethods = "test", enabled = false)
    private void testReplace() throws Exception {
        importSmallWebVisitFile();
        createReplaceWebVisitAction();
        processAnalyzeSkipPublishToS3(CURRENT_PA_TIME.toEpochMilli());
    }

    private void createReplaceWebVisitAction() {
        Action action = new Action();
        action.setType(ActionType.DATA_REPLACE);
        action.setActionInitiator("e2e-test");
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.ActivityStream);
        action.setActionConfiguration(cleanupActionConfiguration);
        action.setTenant(mainTestTenant);
        actionProxy.createAction(mainCustomerSpace, action);
    }

    private void importSmallWebVisitFile() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        // webVisit_46d8cd33-f55d-4fcc-8371-261ebe58fcf9.csv
        mockCSVImport(BusinessEntity.ActivityStream, ADVANCED_MATCH_SUFFIX, 2,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisit));
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void setupTemplates() {
        // setup webvisit template one by one for now, batch setup sometimes having
        // problem
        SimpleTemplateMetadata webVisit = new SimpleTemplateMetadata();
        webVisit.setEntityType(EntityType.WebVisit);
        Set<String> ignoredAttrSet = Sets.newHashSet(InterfaceName.Website.name(), InterfaceName.PostalCode.name());
        webVisit.setIgnoredStandardAttributes(ignoredAttrSet);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(webVisit));
        SimpleTemplateMetadata ptn = new SimpleTemplateMetadata();
        ptn.setEntityType(EntityType.WebVisitPathPattern);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(ptn));
        SimpleTemplateMetadata sm = new SimpleTemplateMetadata();
        sm.setEntityType(EntityType.WebVisitSourceMedium);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(sm));
    }

}
