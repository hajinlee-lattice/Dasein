package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.EntityTypeUtils.generateFullFeedType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

public class ProcessActivityStoreDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String WEBSITE_SYSTEM = "Default_Website_System";
    private static final Instant CURRENT_PA_TIME = LocalDate.of(2017, 8, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private PeriodProxy periodProxy;

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

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        setupTemplates();
        importActivityStoreFiles();
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());

        // run PA with fake current time
        processAnalyzeSkipPublishToS3(CURRENT_PA_TIME.toEpochMilli());
    }

    private void setupTemplates() {
        // setup webvisit template one by one for now, batch setup sometimes having
        // problem
        SimpleTemplateMetadata webVisit = new SimpleTemplateMetadata();
        webVisit.setEntityType(EntityType.WebVisit);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(webVisit));
        SimpleTemplateMetadata ptn = new SimpleTemplateMetadata();
        ptn.setEntityType(EntityType.WebVisitPathPattern);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(ptn));
        SimpleTemplateMetadata sm = new SimpleTemplateMetadata();
        sm.setEntityType(EntityType.WebVisitSourceMedium);
        cdlProxy.createWebVisitTemplate(mainCustomerSpace, Collections.singletonList(sm));
    }

    private void importActivityStoreFiles() {
        // import visit/ptn/source files (20k streams, 4 accounts, 450 days)
        String webVisitCsvFile = "webVisit_d5c08346-1489-4390-b495-59d043305dde.csv";
        String ptnCsvFile = "webVisitPathPtn.csv";
        String smCsvFile = "webVisitSrcMedium.csv";
        importSourceMediumData(smCsvFile);
        importWebVisitData(webVisitCsvFile);
        importWebVisitPtnData(ptnCsvFile);
    }

    private void importWebVisitData(String csvFilename) {
        importData(BusinessEntity.ActivityStream, csvFilename,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisit), false, false);
    }

    private void importSourceMediumData(String csvFilename) {
        importData(BusinessEntity.Catalog, csvFilename,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitSourceMedium), false, false);
    }

    private void importWebVisitPtnData(String csvFilename) {
        importData(BusinessEntity.Catalog, csvFilename,
                generateFullFeedType(WEBSITE_SYSTEM, EntityType.WebVisitPathPattern), false, false);
    }
}
