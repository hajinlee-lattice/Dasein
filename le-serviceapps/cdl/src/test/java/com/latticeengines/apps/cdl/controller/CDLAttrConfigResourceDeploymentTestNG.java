package com.latticeengines.apps.cdl.controller;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
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

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment segment = constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        re = createRuleBasedRatingEngine(retrievedSegment);
    }

    @Test(groups = "deployment")
    public void testGetAttrConfig() {
        testCreate(re);
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), re.getId(), null);
        testRatingEngineNoteCreation(re, true);
        AttrConfigRequest request;
        // my local always exist redis connection issue when call get api with
        // entity account
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Account, true);
        Assert.assertNotNull(request.getAttrConfigs());

        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, true);
        Assert.assertNotNull(request.getAttrConfigs());
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, true);
        Assert.assertNotNull(request.getAttrConfigs());
        System.out.print("Rating:" + JsonUtils.serialize(request));
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.PurchaseHistory,
                true);
        Assert.assertNotNull(request.getAttrConfigs());
    }

    @Test(groups = "deployment")
    public void testVerifyGetTable() {
        List<Table> tables = metadataProxy.getTables(mainTestTenant.getId());
        assertNotNull(tables);
        tables.stream().forEach(table -> {
            assertNotNull(table);
            Long attributeCount = metadataProxy.getTableAttributeCount(mainTestTenant.getId(), table.getName());
            assertEquals(attributeCount.intValue(), table.getAttributes().size());
        });
    }

    @Test(groups = "deployment")
    public void testPartialUpdate() throws InterruptedException {
        AttrConfigRequest request = new AttrConfigRequest();
        AttrConfig config1 = new AttrConfig();
        config1.setAttrName("LastModifiedDate");
        config1.setEntity(BusinessEntity.Contact);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        config1.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Enrichment.name(), enrichProp));
        request.setAttrConfigs(Arrays.asList(config1));
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
        request.setAttrConfigs(Arrays.asList(config2));
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
        request.setAttrConfigs(Arrays.asList(config2));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        // wait the replication lag
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Contact, false);
        customerConfigs = request.getAttrConfigs();
        assertEquals(customerConfigs.size(), 1);
        dateConfig = customerConfigs.get(0);
        assertEquals(dateConfig.getAttrName(), "LastModifiedDate");
        assertEquals(dateConfig.getAttrProps().size(), 1);
        assertEquals(dateConfig.getAttrProps().containsKey(ColumnSelection.Predefined.Segment.name()), false);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testPartialUpdate" })
    public void testDeleteConfigWhenEmptyProps() throws Exception {

        AttrConfigRequest request = new AttrConfigRequest();
        // save two configs, make sure added and no impacts to existing
        AttrConfig config3 = new AttrConfig();
        config3.setAttrName("Email");
        config3.setEntity(BusinessEntity.Contact);
        AttrConfigProp<Boolean> modelProp = new AttrConfigProp<>();
        modelProp.setCustomValue(Boolean.TRUE);
        config3.setAttrProps(ImmutableMap.of(ColumnSelection.Predefined.Model.name(), modelProp));

        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, true);
        Assert.assertEquals(request.getAttrConfigs().size() > 0, true);
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
        request.setAttrConfigs(Arrays.asList(config4));
        cdlAttrConfigProxy.saveAttrConfig(mainTestTenant.getId(), request, AttrConfigUpdateMode.Usage);
        Thread.sleep(500L);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(mainTestTenant.getId(), BusinessEntity.Rating, false);
        assertEquals(request.getAttrConfigs().size(), 0);
    }

    private void testCreate(RatingEngine re) {
        RatingEngineAndActionDTO createdReAndActionDTO = ratingEngineProxy
                .createOrUpdateRatingEngineAndActionDTO(mainTestTenant.getId(), re);
        Assert.assertNotNull(createdReAndActionDTO);
        RatingEngine createdRe = createdReAndActionDTO.getRatingEngine();
        Assert.assertNotNull(createdRe);
        Action action = createdReAndActionDTO.getAction();
        re.setId(createdRe.getId());
        Assert.assertNotNull(createdRe.getActiveModel());
        RatingEngine retrievedRe = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), createdRe.getId());

        if (retrievedRe.getActiveModel() instanceof RuleBasedModel) {
            Assert.assertNotNull(retrievedRe.getActiveModel());
            RuleBasedModel ruModel = (RuleBasedModel) retrievedRe.getActiveModel();
            Assert.assertNotNull(ruModel);
            Assert.assertNotNull(ruModel.getSelectedAttributes());
            Assert.assertTrue(ruModel.getSelectedAttributes().size() > 0);
            if (shouldCreateActionWithRatingEngine1) {
                assertRatingEngineActivationAction(action, createdRe);
            } else {
                Assert.assertNull(action);
            }
        } else if (retrievedRe.getActiveModel() instanceof AIModel) {
            AIModel aiModel = (AIModel) retrievedRe.getActiveModel();
            Assert.assertNotNull(aiModel);
            if (shouldCreateActionWithRatingEngine2) {
                // do nothing for now
            } else {
                Assert.assertNull(action);
            }
        }
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

    private void testRatingEngineNoteCreation(RatingEngine ratingEngine, boolean shouldHaveRatingEngineNote) {
        if (shouldHaveRatingEngineNote) {
            List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                    ratingEngine.getId());
            Assert.assertNotNull(ratingEngineNotes);
            Assert.assertEquals(ratingEngineNotes.size(), 1);
            Assert.assertEquals(ratingEngineNotes.get(0).getNotesContents(), RATING_ENGINE_NOTE_1);
        } else {
            List<RatingEngineNote> ratingEngineNotes = ratingEngineProxy.getAllNotes(mainTestTenant.getId(),
                    ratingEngine.getId());
            Assert.assertTrue(CollectionUtils.isEmpty(ratingEngineNotes));
        }
    }

    private void assertRatingEngineActivationAction(Action action, RatingEngine ratingEngine) {
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertEquals(action.getActionInitiator(), ratingEngine.getCreatedBy());
        Assert.assertTrue(action.getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getRatingEngineId(),
                ratingEngine.getId());
        Assert.assertEquals(((RatingEngineActionConfiguration) action.getActionConfiguration()).getSubType(),
                RatingEngineActionConfiguration.SubType.ACTIVATION);
        Assert.assertEquals(ratingEngine.getStatus(), RatingEngineStatus.ACTIVE);
    }

}
