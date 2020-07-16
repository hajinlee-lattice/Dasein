package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.EntityTypeUtils.generateFullFeedType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;

public class ProcessActivityStoreDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessActivityStoreDeploymentTestNG.class);

    private static final String SKIP_WEB_VISIT = "SKIP_WEB_VISIT";
    private static final String SKIP_OPPORTUNITY = "SKIP_OPPORTUNITY";
    private static final String SKIP_MARKETING = "SKIP_MARKETING";
    private static final String SKIP_INTENT = "SKIP_INTENT";
    private static final String WEBSITE_SYSTEM = "Default_Website_System";
    private static final String OPPORTUNITY_SYSTEM = "Default_Opportunity_System";
    private static final String INTENT_SYSTEM = "Default_DnbIntent_System";
    private static final String MARKETO_SYSTEM = "Default_Marketo_System";
    private static final String ELOQUA_SYSTEM = "Default_Eloqua_System";
    protected static final Instant CURRENT_PA_TIME = LocalDate.of(2017, 8, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private TimeLineProxy timeLineProxy;

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
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        // TODO - simplify skipping logic
        if (!Boolean.parseBoolean(System.getenv(SKIP_WEB_VISIT))) {
            setupWebVisit();
        } else {
            log.warn("Skip web visit setup. {}={}", SKIP_WEB_VISIT, System.getenv(SKIP_WEB_VISIT));
        }
        if (!Boolean.parseBoolean(System.getenv(SKIP_OPPORTUNITY))) {
            // FIXME enable opportunity data again after test data is updated to the new
            // schema
            // setupOpportunityTemplates();
        } else {
            log.info("Skip opportunity setup. {}={}", SKIP_OPPORTUNITY, System.getenv(SKIP_OPPORTUNITY));
        }
        if (!Boolean.parseBoolean(System.getenv(SKIP_MARKETING))) {
            setupMarketingTemplates();
        } else {
            log.info("Skip marketing setup. {}={}", SKIP_MARKETING, System.getenv(SKIP_MARKETING));
        }
        if (!Boolean.parseBoolean(System.getenv(SKIP_INTENT))) {
            setupIntentTemplates();
        } else {
            log.info("Skip intent setup. {}={}", SKIP_INTENT, System.getenv(SKIP_INTENT));
        }
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
//        setupTimeline();
        if (isLocalEnvironment()) {
            // run PA with fake current time
            processAnalyzeSkipPublishToS3(CURRENT_PA_TIME.toEpochMilli());
        } else {
            runTestWithRetry(getCandidateFailingSteps(), CURRENT_PA_TIME.toEpochMilli());
        }

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

    private void setupOpportunityTemplates() throws Exception {
        createOpportunitySystem();
        Thread.sleep(2000L);
        Assert.assertTrue(createS3Folder(OPPORTUNITY_SYSTEM, Arrays.asList(EntityType.Opportunity,
                EntityType.OpportunityStageName)));

        // setup templates
        boolean created = cdlProxy.createDefaultOpportunityTemplate(mainCustomerSpace, OPPORTUNITY_SYSTEM);
        Assert.assertTrue(created,
                String.format("Failed to create opportunity template in system %s", OPPORTUNITY_SYSTEM));

        // data: opportunity_1ec2c252-f36d-4054-aaf4-9d6379006244.csv
        // stats: opportunity_1ec2c252-f36d-4054-aaf4-9d6379006244.json
        mockCSVImport(BusinessEntity.ActivityStream, ADVANCED_MATCH_SUFFIX, 3,
                generateFullFeedType(OPPORTUNITY_SYSTEM, EntityType.Opportunity));
        Thread.sleep(2000);
        // opportunity_stage.csv
        mockCSVImport(BusinessEntity.Catalog, ADVANCED_MATCH_SUFFIX, 4,
                generateFullFeedType(OPPORTUNITY_SYSTEM, EntityType.OpportunityStageName));
        Thread.sleep(2000);
    }

    private void setupMarketingTemplates() throws Exception {
        createMarketingActivitySystems();
        Thread.sleep(2000L);
        Assert.assertTrue(createS3Folder(MARKETO_SYSTEM, Arrays.asList(EntityType.MarketingActivity, EntityType.MarketingActivityType)));

        // setup templates
        Assert.assertTrue(cdlProxy.createDefaultMarketingTemplate(mainCustomerSpace, MARKETO_SYSTEM, S3ImportSystem.SystemType.Marketo.name()),
                String.format("Failed to create marketing template in system %s", MARKETO_SYSTEM));

        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.ActivityStream, ADVANCED_MATCH_SUFFIX, 4,
                generateFullFeedType(MARKETO_SYSTEM, EntityType.MarketingActivity));
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Catalog, ADVANCED_MATCH_SUFFIX, 5,
                generateFullFeedType(MARKETO_SYSTEM, EntityType.MarketingActivityType));
        Thread.sleep(2000);
    }

    private void setupIntentTemplates() throws Exception {
        Thread.sleep(2000L);
        Assert.assertTrue(cdlProxy.createDefaultDnbIntentDataTemplate(mainCustomerSpace),
                String.format("Failed to create intent template in system %s", INTENT_SYSTEM));

        mockCSVImport(BusinessEntity.ActivityStream, ADVANCED_MATCH_SUFFIX, 5,
                generateFullFeedType(INTENT_SYSTEM, EntityType.CustomIntent));
        Thread.sleep(2000);
    }

    /*-
     * create a dummy system for opportunity templates to attach to
     * NOTE that account import for this system might not work (not fully setup)
     * TODO make account importable and make sure account in template are linked to account from opportunity
     */
    private void createOpportunitySystem() {
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        system.setName(OPPORTUNITY_SYSTEM);
        system.setDisplayName(OPPORTUNITY_SYSTEM);
        system.setSystemType(S3ImportSystem.SystemType.Other);
        system.setPriority(2);
        // dummy id, maybe just use the customer account id
        system.setAccountSystemId(String.format("user_%s_dlugenoz_AccountId", OPPORTUNITY_SYSTEM));
        system.setMapToLatticeAccount(true);
        cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
    }

    private void createMarketingActivitySystems() {
        createMarketingActivitySystem(MARKETO_SYSTEM);
//        createMarketingActivitySystem(ELOQUA_SYSTEM);
    }

    private void createMarketingActivitySystem(String systemName) {
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


    private void setupWebVisit() throws Exception {
        setupWebVisitTemplates();
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
    }

    private void setupWebVisitTemplates() {
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

    private void setupTimeline() {
        preparetimeline1();
        preparetimeline2();
        preparetimeline3();
    }

    private void preparetimeline1() {
        String timelineName1 = "timelineName1";
        TimeLine timeLine1 = new TimeLine();
        timeLine1.setName(timelineName1);
        timeLine1.setTimelineId(String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace), timelineName1));
        timeLine1.setEntity(BusinessEntity.Account.name());
        timeLine1.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.MarketingActivity));
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));

        timeLine1.setEventMappings(mappingMap);
        timeLineProxy.createTimeline(mainCustomerSpace, timeLine1);
    }

    private void preparetimeline2() {
        String timelineName = "timelineName2";
        TimeLine timeLine2 = new TimeLine();
        timeLine2.setName(timelineName);
        timeLine2.setTimelineId(String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace), timelineName));
        timeLine2.setEntity(BusinessEntity.Contact.name());
        timeLine2.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.MarketingActivity));
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));

        timeLine2.setEventMappings(mappingMap);
        timeLineProxy.createTimeline(mainCustomerSpace, timeLine2);
    }

    private void preparetimeline3() {
        String timelineName = "timelineName3";

        TimeLine timeLine3 = new TimeLine();
        timeLine3.setName(timelineName);
        timeLine3.setTimelineId(String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace), timelineName));
        timeLine3.setEntity(BusinessEntity.Account.name());
        timeLine3.setStreamTypes(Arrays.asList(AtlasStream.StreamType.Opportunity, AtlasStream.StreamType.WebVisit));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        mappingMap.put(AtlasStream.StreamType.WebVisit.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.WebVisit));
        mappingMap.put(AtlasStream.StreamType.Opportunity.name(),
                TimeLineStoreUtils.getTimelineStandardMappingByStreamType(AtlasStream.StreamType.Opportunity));

        timeLine3.setEventMappings(mappingMap);
        timeLineProxy.createTimeline(mainCustomerSpace, timeLine3);
    }

    protected List<String> getCandidateFailingSteps() {
        return Arrays.asList(
                "aggActivityStreamToDaily",
                "periodStoresGenerationStep",
                "metricsGroupsGenerationStep",
                "mergeActivityMetricsToEntityStep",
                "profileAccountActivityMetricsStep",
                "profileContactActivityMetricsStep",
                "combineStatistics", //
                "exportToRedshift", //
                "generateProcessingReport", // mimic failed in scoring
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "exportProcessAnalyzeToS3", //
                "commitEntityMatch", //
                "finishProcessing");
    }

}
