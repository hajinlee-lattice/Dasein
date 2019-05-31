package com.latticeengines.apps.cdl.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class CDLAttrConfigResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLAttrConfigResourceDeploymentTestNG.class);
    private static final String RATING_ENGINE_NOTE_1 = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private final boolean shouldCreateActionWithRatingEngine1 = true;
    private final boolean shouldCreateActionWithRatingEngine2 = false;
    @Inject
    private RatingEngineProxy ratingEngineProxy;
    @Inject
    private CDLTestDataService cdlTestDataService;
    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;
    @Inject
    private MetadataProxy metadataProxy;

    private RatingEngine re;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        MetadataSegment segment = constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        re = createRuleBasedRatingEngine(retrievedSegment);
    }

    @Test(groups = "deployment-app")
    public void testGetAttrConfig() {
        createRatingEngine(re);
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), re.getId(), null);
        AttrConfigRequest request;
        // my local always exist redis connection issue when call get api with
        // entity account
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Account, true);
        Assert.assertNotNull(request.getAttrConfigs());

        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, true);
        Assert.assertNotNull(request.getAttrConfigs());
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, true);
        Assert.assertNotNull(request.getAttrConfigs());
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.PurchaseHistory,
                true);
        Assert.assertNotNull(request.getAttrConfigs());
    }

    @Test(groups = "deployment-app")
    public void testVerifyGetTable() {
        List<Table> tables = metadataProxy.getTables(mainTestTenant.getId());
        assertNotNull(tables);
        tables.forEach(table -> {
            assertNotNull(table);
            Long attributeCount = metadataProxy.getTableAttributeCount(mainTestTenant.getId(), table.getName());
            assertEquals(attributeCount.intValue(), table.getAttributes().size());
        });
    }

    @Test(groups = "deployment-app")
    public void testPartialUpdate() throws InterruptedException {
        AttrConfigRequest request = new AttrConfigRequest();
        AttrConfig config1 = new AttrConfig();
        config1.setAttrName("LastModifiedDate");
        config1.setEntity(BusinessEntity.Contact);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        config1.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Enrichment.name(), enrichProp));
        request.setAttrConfigs(Collections.singletonList(config1));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        // wait the replication lag
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        List<AttrConfig> customerConfigs = request.getAttrConfigs();
        assertEquals(customerConfigs.size(), 1);

        AttrConfig config2 = new AttrConfig();
        config2.setAttrName("LastModifiedDate");
        config2.setEntity(BusinessEntity.Contact);
        AttrConfigProp<Boolean> segmentProp = new AttrConfigProp<>();
        segmentProp.setCustomValue(Boolean.TRUE);
        config2.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Segment.name(), segmentProp));
        request.setAttrConfigs(Collections.singletonList(config2));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        // wait the replication lag
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        customerConfigs = request.getAttrConfigs();
        assertEquals(customerConfigs.size(), 1);
        AttrConfig dateConfig = customerConfigs.get(0);
        assertEquals(dateConfig.getAttrName(), "LastModifiedDate");
        assertEquals(dateConfig.getAttrProps().size(), 2);

        // transfer null, make sure the prop removed
        segmentProp = new AttrConfigProp<>();
        segmentProp.setCustomValue(null);
        config2.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Segment.name(), segmentProp));
        request.setAttrConfigs(Collections.singletonList(config2));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        // wait the replication lag
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        customerConfigs = request.getAttrConfigs();
        assertEquals(customerConfigs.size(), 1);
        dateConfig = customerConfigs.get(0);
        assertEquals(dateConfig.getAttrName(), "LastModifiedDate");
        assertEquals(dateConfig.getAttrProps().size(), 1);
        assertFalse(dateConfig.getAttrProps().containsKey(ColumnSelection.Predefined.Segment.name()));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testPartialUpdate" })
    public void testDeleteConfigWhenEmptyProps() throws Exception {
        // save two configs, make sure added and no impacts to existing
        AttrConfig config3 = new AttrConfig();
        config3.setAttrName("Email");
        config3.setEntity(BusinessEntity.Contact);
        AttrConfigProp<Boolean> modelProp = new AttrConfigProp<>();
        modelProp.setCustomValue(Boolean.TRUE);
        config3.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.TalkingPoint.name(), modelProp));

        AttrConfigRequest request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(),
                BusinessEntity.Rating, true);
        Assert.assertTrue(request.getAttrConfigs().size() > 0);
        AttrConfig config4 = request.getAttrConfigs().get(0);
        config4.setEntity(BusinessEntity.Rating);
        AttrConfigProp<Boolean> SegmentProp = new AttrConfigProp<>();
        SegmentProp.setCustomValue(Boolean.TRUE);
        config4.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Segment.name(), SegmentProp));
        request.setAttrConfigs(Arrays.asList(config3, config4));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        assertEquals(request.getAttrConfigs().size(), 2);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, false);
        assertEquals(request.getAttrConfigs().size(), 1);

        // db only has one rating object for the tenant, save one config with
        // entity Rating and TalkingPoint customer value null , at last verify
        // remove the config
        SegmentProp = new AttrConfigProp<>();
        SegmentProp.setCustomValue(null);
        config4.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Segment.name(), SegmentProp));
        request.setAttrConfigs(Collections.singletonList(config4));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, false);
        assertEquals(request.getAttrConfigs().size(), 0);

    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testDeleteConfigWhenEmptyProps" })
    public void testCleanupAttrConfigForTenant() throws Exception {
        cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(mainTestTenant.getId(), BusinessEntity.Account);
        Thread.sleep(500L);
        AttrConfigRequest request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(),
                BusinessEntity.Account, false);
        assertEquals(request.getAttrConfigs().size(), 0);

        cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(mainTestTenant.getId(), null);
        Thread.sleep(500L);

        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(),
                BusinessEntity.Account, false);
        assertEquals(request.getAttrConfigs().size(), 0);

        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        assertEquals(request.getAttrConfigs().size(), 0);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, false);
        assertEquals(request.getAttrConfigs().size(), 0);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.PurchaseHistory,
                false);
        assertEquals(request.getAttrConfigs().size(), 0);
    }

    private void createRatingEngine(RatingEngine re) {
        RatingEngine createdRe = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re);
        Assert.assertNotNull(createdRe);
        re.setId(createdRe.getId());
    }

    private RatingEngine createRuleBasedRatingEngine(MetadataSegment retrievedSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE_1);
        if (shouldCreateActionWithRatingEngine1) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        }
        return ratingEngine;
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCleanupAttrConfigForTenant" })
    public void testAttrLimit() throws Exception {
        // write HG limit to -100, run PA, will throw exception
        Path configPathForHG = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(mainCustomerSpace), "PLS")
                .append(new Path("/DataCloudLicense").append(new Path("/HG")));
        Camille camille = CamilleEnvironment.getCamille();
        camille.upsert(configPathForHG, DocumentUtils.toRawDocument(-100), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.info("Start processing and analyzing ...");
        try {
            cdlProxy.processAnalyze(mainTestTenant.getId(), null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
            Assert.assertTrue(e.getMessage().endsWith("User activate or enable more allowed attribute."));
        }
    }
}
