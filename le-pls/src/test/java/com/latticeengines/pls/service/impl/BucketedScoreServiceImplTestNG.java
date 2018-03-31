package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.service.TenantService;

public class BucketedScoreServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final String TENANT2 = "TENANT2";

    private static String MODEL_ID;
    private ModelSummary modelSummary;
    private Tenant tenant1 = new Tenant();
    private Tenant tenant2 = new Tenant();

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private TenantService tenantService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupTenants();

        MultiTenantContext.setTenant(tenant1);
        MODEL_ID = UuidUtils.shortenUuid(UUID.randomUUID());
        modelSummary = BucketedScoreServiceTestUtils.createModelSummary(MODEL_ID);
        modelSummaryService.createModelSummary(modelSummary, tenant1.getId());
    }

    @Test(groups = { "functional" })
    public void createBucketedScoreSummary() throws Exception {
        InputStream is = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/BucketedScoreSummary/data/part-00000.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(is);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(records);
        bucketedScoreService.createOrUpdateBucketedScoreSummary(modelSummary.getId(), bucketedScoreSummary);
        BucketedScoreSummary retrieved = bucketedScoreService.getBucketedScoreSummaryForModelId(modelSummary.getId());
        assertEquals(retrieved.getTotalNumConverted(), 878);
        bucketedScoreSummary.setTotalNumConverted(bucketedScoreSummary.getTotalNumConverted() + 2);
        bucketedScoreSummary.setModelSummary(null);
        bucketedScoreService.createOrUpdateBucketedScoreSummary(modelSummary.getId(), bucketedScoreSummary);
        retrieved = bucketedScoreService.getBucketedScoreSummaryForModelId(modelSummary.getId());
        assertEquals(retrieved.getTotalNumConverted(), 880);
        System.out.println(bucketedScoreSummary.getBucketedScores()[4]);
        System.out.println(retrieved.getBucketedScores()[4]);
        assertEquals(bucketedScoreSummary.getTotalNumConverted(), retrieved.getTotalNumConverted());
        assertEquals(bucketedScoreSummary.getOverallLift(), retrieved.getOverallLift());
        assertEquals(bucketedScoreSummary.getTotalNumLeads(), retrieved.getTotalNumLeads());
        assertNotNull(retrieved.getBarLifts());
        assertNotNull(retrieved.getBucketedScores());
    }

    @Test(groups = { "functional" })
    public void createGroupOfBucketMetadataForModel_assertCreated() throws Exception {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(MODEL_ID);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        bucketedScoreService.createBucketMetadatas(MODEL_ID,
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata1));

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimes(MODEL_ID);
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        modelSummary = modelSummaryService.getModelSummary(MODEL_ID);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        System.out.println("newLastUpdateTime is " + newLastUpdateTime);
        assertTrue(newLastUpdateTime > oldLastUpdateTime);
        BucketedScoreServiceTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(timestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createGroupOfBucketMetadataForModel_assertCreated")
    public void createAnotherGroupsOfBucketMetadata_assertCreated() throws Exception {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(MODEL_ID);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        bucketedScoreService.createBucketMetadatas(MODEL_ID,
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata2));
        modelSummary = modelSummaryService.getModelSummary(MODEL_ID);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);

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

        BucketedScoreServiceTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(earlierTimestamp));
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(creationTimeToBucketMetadatas.get(laterTimestamp));
    }

    @Test(groups = { "functional" }, dependsOnMethods = "createAnotherGroupsOfBucketMetadata_assertCreated")
    public void testGetUpToDateModelBucketMetadata() throws Exception {
        List<BucketMetadata> bucketMetadatas = bucketedScoreService.getUpToDateModelBucketMetadata(MODEL_ID);
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

    @Test(groups = { "functional" }, dependsOnMethods = "testGetUpToDateModelBucketMetadata")
    public void testGetUpToDateModelBucketMetadataAcrossTenants() throws Exception {
        setupSecurityContext(tenant2);
        List<BucketMetadata> bucketMetadatas = bucketedScoreService
                .getUpToDateModelBucketMetadataAcrossTenants(MODEL_ID);
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

    private void setupTenants() throws Exception {
        if (tenantService.findByTenantId(TENANT1) != null) {
            tenantService.discardTenant(tenantService.findByTenantId(TENANT1));
        }
        if (tenantService.findByTenantId(TENANT2) != null) {
            tenantService.discardTenant(tenantService.findByTenantId(TENANT2));
        }
        tenant1.setId(TENANT1);
        tenant1.setName(TENANT1);
        tenantService.registerTenant(tenant1);
        tenant2.setId(TENANT2);
        tenant2.setName(TENANT2);
        tenantService.registerTenant(tenant2);

        setupSecurityContext(tenant1);
    }

}
