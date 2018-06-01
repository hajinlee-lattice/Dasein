package com.latticeengines.pls.controller;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.util.BucketedMetadataTestUtils;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

public class BucketedScoreResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreResourceDeploymentTestNG.class);

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private MetadataSegment segment;

    private RatingEngine re1;

    private String modelGuid;
    private ModelSummary modelSummary;

    @Inject
    private MetadataSegmentService metadataSegmentService;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);
        segment = RatingEngineResourceDeploymentTestNG.constructSegment(SEGMENT_NAME);
        MetadataSegment createdSegment = metadataSegmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = metadataSegmentService.getSegmentByName(createdSegment.getName(), false);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        re1 = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(),
                createAIRatingEngine(retrievedSegment, RatingEngineType.CROSS_SELL));

        modelGuid = String.format("ms__%s__LETest", UUID.randomUUID().toString());
        modelSummary = createModelSummary(modelGuid, mainTestTenant);
        modelSummaryEntityMgr.create(modelSummary);

        AIModel ratingModel = (AIModel) ratingEngineProxy.getRatingModel(mainTestTenant.getId(), re1.getId(),
                re1.getActiveModel().getId());
        ratingModel.setModelSummaryId(modelGuid);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), re1.getId(), ratingModel.getId(), ratingModel);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testCreate() throws InterruptedException {
        List<BucketMetadata> list = BucketedMetadataTestUtils.generateDefaultBucketMetadataList();
        restTemplate.postForObject(getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/ratingengine/" + re1.getId()
                + "/model/" + modelGuid, list, Void.class);
        Thread.sleep(500);

        Map<Long, List<BucketMetadata>> history = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/ratingengine/" + re1.getId(), Map.class);
        Assert.assertNotNull(history);
        Assert.assertEquals(history.size(), 1);

        List<BucketMetadata> bucketList = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/uptodate/ratingengine/" + re1.getId(),
                List.class);
        Assert.assertNotNull(bucketList);
        Assert.assertEquals(bucketList.size(), list.size());

        // create another bucketlist and store it
        list = BucketedMetadataTestUtils.generateDefaultBucketMetadataList().subList(0, list.size() - 1);
        restTemplate.postForObject(getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/ratingengine/" + re1.getId()
                + "/model/" + modelGuid, list, Void.class);
        Thread.sleep(500);
        bucketList = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/uptodate/ratingengine/" + re1.getId(),
                List.class);
        Assert.assertNotNull(bucketList);
        Assert.assertEquals(bucketList.size(), list.size());
    }

    private RatingEngine createAIRatingEngine(MetadataSegment retrievedSegment, RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(type);
        return ratingEngine;
    }

    private ModelSummary createModelSummary(String modelId, Tenant tenant) throws Exception {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId(modelId);
        modelSummary.setDisplayName(modelId);
        modelSummary.setName(modelId);
        modelSummary.setApplicationId("application_id_0000");
        modelSummary.setRocScore(0.75);
        modelSummary.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        modelSummary.setTrainingRowCount(8000L);
        modelSummary.setTestRowCount(2000L);
        modelSummary.setTotalRowCount(10000L);
        modelSummary.setTrainingConversionCount(80L);
        modelSummary.setTestConversionCount(20L);
        modelSummary.setTotalConversionCount(100L);
        modelSummary.setConstructionTime(System.currentTimeMillis());
        if (modelSummary.getConstructionTime() == null) {
            modelSummary.setConstructionTime(System.currentTimeMillis());
        }
        modelSummary.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummary.setLastUpdateTime(modelSummary.getConstructionTime());
        setDetails(modelSummary);
        modelSummary.setTenant(tenant);
        return modelSummary;
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-marketo-UI-issue.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

}
