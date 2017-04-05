package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class BucketedScoreServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final BucketMetadata BUCKET_METADATA_A = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_B = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_C = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_D = new BucketMetadata();

    private static final BucketMetadata BUCKET_METADATA_A_1 = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_B_1 = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_C_1 = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_D_1 = new BucketMetadata();
    private static final BucketMetadata BUCKET_METADATA_E_1 = new BucketMetadata();

    private static final Double LIFT_1 = 3.4;
    private static final Double LIFT_2 = 2.4;
    private static final Double LIFT_3 = 1.2;
    private static final Double LIFT_4 = 0.4;
    private static final Double LIFT_5 = 1.5;
    private static final int NUM_LEADS_BUCKET_1 = 28588;
    private static final int NUM_LEADS_BUCKET_2 = 14534;
    private static final int NUM_LEADS_BUCKET_3 = 25206;
    private static final int NUM_LEADS_BUCKET_4 = 25565;
    private static final int NUM_LEADS_BUCKET_5 = 10000;
    private static final String MODEL_ID = "BUCKET_SCORES_MODEL_SUMMARY";
    private ModelSummary modelSummary = new ModelSummary();
    private Tenant tenant = new Tenant();

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private TenantService tenantService;

    private void setupTenant(String tenantId) throws Exception {
        if (tenantService.findByTenantId(tenantId) != null) {
            tenantService.discardTenant(tenantService.findByTenantId(tenantId));
        }
        tenant.setId(tenantId);
        tenant.setName(tenantId);
        tenantService.registerTenant(tenant);

        setupSecurityContext(tenant);
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

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupTenant(TENANT1);
        cleanupBucketMetadataDB();

        MultiTenantContext.setTenant(tenant);
        modelSummary.setId(MODEL_ID);
        modelSummary.setDisplayName(MODEL_ID);
        modelSummary.setName(MODEL_ID);
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
        setDetails(modelSummary);
        modelSummary.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummaryService.createModelSummary(modelSummary, tenant.getId());
    }

    {
        BUCKET_METADATA_A.setBucket(BucketName.A);
        BUCKET_METADATA_A.setNumLeads(NUM_LEADS_BUCKET_1);
        BUCKET_METADATA_A.setLeftBoundScore(95);
        BUCKET_METADATA_A.setRightBoundScore(99);
        BUCKET_METADATA_A.setLift(LIFT_1);
        BUCKET_METADATA_B.setBucket(BucketName.B);
        BUCKET_METADATA_B.setNumLeads(NUM_LEADS_BUCKET_2);
        BUCKET_METADATA_B.setLeftBoundScore(85);
        BUCKET_METADATA_B.setRightBoundScore(95);
        BUCKET_METADATA_B.setLift(LIFT_2);
        BUCKET_METADATA_C.setBucket(BucketName.C);
        BUCKET_METADATA_C.setNumLeads(NUM_LEADS_BUCKET_3);
        BUCKET_METADATA_C.setLeftBoundScore(50);
        BUCKET_METADATA_C.setRightBoundScore(85);
        BUCKET_METADATA_C.setLift(LIFT_3);
        BUCKET_METADATA_D.setBucket(BucketName.D);
        BUCKET_METADATA_D.setNumLeads(NUM_LEADS_BUCKET_4);
        BUCKET_METADATA_D.setLeftBoundScore(5);
        BUCKET_METADATA_D.setRightBoundScore(50);
        BUCKET_METADATA_D.setLift(LIFT_4);

        BUCKET_METADATA_A_1.setBucket(BucketName.A);
        BUCKET_METADATA_A_1.setNumLeads(NUM_LEADS_BUCKET_1);
        BUCKET_METADATA_A_1.setLeftBoundScore(95);
        BUCKET_METADATA_A_1.setRightBoundScore(99);
        BUCKET_METADATA_A_1.setLift(LIFT_1);
        BUCKET_METADATA_B_1.setBucket(BucketName.B);
        BUCKET_METADATA_B_1.setNumLeads(NUM_LEADS_BUCKET_2);
        BUCKET_METADATA_B_1.setLeftBoundScore(85);
        BUCKET_METADATA_B_1.setRightBoundScore(95);
        BUCKET_METADATA_B_1.setLift(LIFT_2);
        BUCKET_METADATA_C_1.setBucket(BucketName.C);
        BUCKET_METADATA_C_1.setNumLeads(NUM_LEADS_BUCKET_3);
        BUCKET_METADATA_C_1.setLeftBoundScore(50);
        BUCKET_METADATA_C_1.setRightBoundScore(85);
        BUCKET_METADATA_C_1.setLift(LIFT_3);
        BUCKET_METADATA_D_1.setBucket(BucketName.D);
        BUCKET_METADATA_D_1.setNumLeads(NUM_LEADS_BUCKET_4);
        BUCKET_METADATA_D_1.setLeftBoundScore(30);
        BUCKET_METADATA_D_1.setRightBoundScore(50);
        BUCKET_METADATA_D_1.setLift(LIFT_4);
        BUCKET_METADATA_E_1.setBucket(BucketName.A_PLUS);
        BUCKET_METADATA_E_1.setNumLeads(NUM_LEADS_BUCKET_5);
        BUCKET_METADATA_E_1.setLeftBoundScore(5);
        BUCKET_METADATA_E_1.setRightBoundScore(30);
        BUCKET_METADATA_E_1.setLift(LIFT_5);
    }

    @Test(groups = { "functional" })
    public void createGroupOfBucketMetadataForModel_assertCreated() throws Exception {
        bucketedScoreService.createBucketMetadatas(MODEL_ID,
                Arrays.asList(BUCKET_METADATA_A, BUCKET_METADATA_B, BUCKET_METADATA_C, BUCKET_METADATA_D));

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(MODEL_ID);
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(timestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createGroupOfBucketMetadataForModel_assertCreated")
    public void createAnotherGroupsOfBucketMetadata_assertCreated() throws Exception {
        bucketedScoreService.createBucketMetadatas(MODEL_ID, Arrays.asList(BUCKET_METADATA_A_1, BUCKET_METADATA_B_1,
                BUCKET_METADATA_C_1, BUCKET_METADATA_D_1, BUCKET_METADATA_E_1));

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(MODEL_ID);
        assertEquals(creationTimeToBucketMetadatas.keySet().size(), 2);
        Long earlierTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0],
                laterTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[1];
        Long placeHolderTimestamp;
        if (earlierTimestamp > laterTimestamp) {
            placeHolderTimestamp = earlierTimestamp;
            earlierTimestamp = laterTimestamp;
            laterTimestamp = placeHolderTimestamp;
        }

        testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(earlierTimestamp));
        testSecondGroupBucketMetadata(creationTimeToBucketMetadatas.get(laterTimestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createAnotherGroupsOfBucketMetadata_assertCreated")
    public void testGetUpToDateModelBucketMetadata() throws Exception {
        List<BucketMetadata> bucketMetadatas = bucketedScoreService.getUpToDateModelBucketMetadata(MODEL_ID);
        testSecondGroupBucketMetadata(bucketMetadatas);
    }

    private void testFirstGroupBucketMetadata(List<BucketMetadata> bucketMetadataList) {
        assertEquals(bucketMetadataList.size(), 4);
        Set<BucketName> bucketNames = new HashSet<>(
                Arrays.asList(BucketName.A, BucketName.B, BucketName.C, BucketName.D));
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            switch (bucketMetadata.getBucket()) {
            case A:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_1);
                assertEquals(bucketMetadata.getLeftBoundScore(), 95);
                assertEquals(bucketMetadata.getRightBoundScore(), 99);
                assertEquals(bucketMetadata.getLift(), LIFT_1);
                break;
            case B:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_2);
                assertEquals(bucketMetadata.getLeftBoundScore(), 85);
                assertEquals(bucketMetadata.getRightBoundScore(), 95);
                assertEquals(bucketMetadata.getLift(), LIFT_2);
                break;
            case C:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_3);
                assertEquals(bucketMetadata.getLeftBoundScore(), 50);
                assertEquals(bucketMetadata.getRightBoundScore(), 85);
                assertEquals(bucketMetadata.getLift(), LIFT_3);
                break;
            case D:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_4);
                assertEquals(bucketMetadata.getLeftBoundScore(), 5);
                assertEquals(bucketMetadata.getRightBoundScore(), 50);
                assertEquals(bucketMetadata.getLift(), LIFT_4);
                break;
            default:
                assertTrue(false);
                break;
            }
        }
        assertTrue(bucketNames.isEmpty());
    }

    private void testSecondGroupBucketMetadata(List<BucketMetadata> bucketMetadataList) {
        assertEquals(bucketMetadataList.size(), 5);
        Set<BucketName> bucketNames = new HashSet<>(
                Arrays.asList(BucketName.A, BucketName.B, BucketName.C, BucketName.D, BucketName.A_PLUS));
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            switch (bucketMetadata.getBucket()) {
            case A:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_1);
                assertEquals(bucketMetadata.getLeftBoundScore(), 95);
                assertEquals(bucketMetadata.getRightBoundScore(), 99);
                assertEquals(bucketMetadata.getLift(), LIFT_1);
                break;
            case B:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_2);
                assertEquals(bucketMetadata.getLeftBoundScore(), 85);
                assertEquals(bucketMetadata.getRightBoundScore(), 95);
                assertEquals(bucketMetadata.getLift(), LIFT_2);
                break;
            case C:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_3);
                assertEquals(bucketMetadata.getLeftBoundScore(), 50);
                assertEquals(bucketMetadata.getRightBoundScore(), 85);
                assertEquals(bucketMetadata.getLift(), LIFT_3);
                break;
            case D:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_4);
                assertEquals(bucketMetadata.getLeftBoundScore(), 30);
                assertEquals(bucketMetadata.getRightBoundScore(), 50);
                assertEquals(bucketMetadata.getLift(), LIFT_4);
                break;
            case A_PLUS:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getNumLeads(), NUM_LEADS_BUCKET_5);
                assertEquals(bucketMetadata.getLeftBoundScore(), 5);
                assertEquals(bucketMetadata.getRightBoundScore(), 30);
                assertEquals(bucketMetadata.getLift(), LIFT_5);
                break;
            default:
                assertTrue(false);
                break;
            }
        }
        assertTrue(bucketNames.isEmpty());
    }

}
