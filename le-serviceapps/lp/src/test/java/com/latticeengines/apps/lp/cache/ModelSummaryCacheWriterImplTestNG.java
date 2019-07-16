package com.latticeengines.apps.lp.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public class ModelSummaryCacheWriterImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private ModelSummaryCacheWriter modelSummaryCacheWriter;

    private List<String> modelSummaryIdsInTenant1 = new ArrayList<>();

    private List<String> modelSummaryIdsInTenant2 = new ArrayList<>();

    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = "functional")
    public void setup() {
        testBed.bootstrap(2);
        tenant1 = testBed.getMainTestTenant();
        tenant2 = testBed.getTestTenants().get(1);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        deleteIdsAndEntitiesByTenant(tenant1);
        deleteIdsAndEntitiesByTenant(tenant2);
        assertTrue(getEntitiesByTenant(tenant1).isEmpty());
        assertTrue(getEntitiesByTenant(tenant2).isEmpty());
    }

    private int summaryCount = 10;

    @Test(groups = {"functional"}, dataProvider = "modelSummariesByTenant")
    public void testSetIdsAndEntitiesByTenant(Tenant tenant, List<ModelSummary> modelSummaries) {
        setIdsAndEntitiesByTenant(tenant, modelSummaries);
        assertNotNull(getEntityById(modelSummaries.get(0).getId()));
    }

    private void verifyModelSummmary(List<String> ids, Tenant tenant) {
        for (String id : ids) {
            ModelSummary modelSummary = getEntityById(id);
            assertNotNull(modelSummary);
            assertEquals(modelSummary.getId(), id);
            assertEquals(modelSummary.getDisplayName(), "displayName");
            assertEquals(modelSummary.getModelType(), "PythonScriptModel");
            if (tenant.getPid().equals(tenant1.getPid())) {
                assertEquals(modelSummary.getStatus(), ModelSummaryStatus.ACTIVE);
            } else {
                assertEquals(modelSummary.getStatus(), ModelSummaryStatus.INACTIVE);
            }
        }
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testSetIdsAndEntitiesByTenant"})
    public void testGetEntityById() {
        verifyModelSummmary(modelSummaryIdsInTenant1, tenant1);
        verifyModelSummmary(modelSummaryIdsInTenant2, tenant2);
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testSetIdsAndEntitiesByTenant"}, dataProvider = "modelSummaryIds")
    public void testGetEntitiesByIds(List<String> ids) {
        List<ModelSummary> modelSummaries = getEntitiesByIds(ids);
        assertEquals(modelSummaries.size(), ids.size());
        for (ModelSummary modelSummary : modelSummaries) {
            assertTrue(ids.contains(modelSummary.getId()));
        }
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testSetIdsAndEntitiesByTenant"}, dataProvider = "tenant")
    public void testGetEntitiesByTenant(Tenant tenant) {
        List<ModelSummary> modelSummaries = getEntitiesByTenant(tenant);
        assertEquals(modelSummaries.size(), 20);
        ModelSummaryStatus status = tenant.getPid().equals(tenant1.getPid()) ? ModelSummaryStatus.ACTIVE :
                ModelSummaryStatus.INACTIVE;
        for (ModelSummary modelSummary : modelSummaries) {
            assertEquals(modelSummary.getDisplayName(), "displayName");
            assertEquals(modelSummary.getModelType(), "PythonScriptModel");
            assertEquals(modelSummary.getStatus(), status);
        }
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testGetEntityById", "testGetEntitiesByIds", "testGetEntitiesByTenant"})
    public void testSetEntitiesIdsByTenant() {
        List<ModelSummary> modelSummaries = generateModelSummaries(tenant2.getPid(), 40l, ModelSummaryStatus.ACTIVE);
        setEntitiesIdsByTenant(tenant2, modelSummaries);
        List<ModelSummary> modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 20);

        EntityListCache entityListCache = getEntitiesAndNonExistEntitityIdsByTenant(tenant2);
        assertEquals(entityListCache.getExistEntities().size(), 20);
        assertEquals(entityListCache.getNonExistIds().size(), 10);

        setEntities(modelSummaries);
        modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 30);

        List<String> ids = modelSummaries.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList());
        deleteEntitiesByIds(ids);
        entityListCache = getEntitiesAndNonExistEntitityIdsByTenant(tenant2);
        assertEquals(entityListCache.getExistEntities().size(), 20);
        assertEquals(entityListCache.getNonExistIds().size(), 10);
        deleteIdsByTenantId(tenant2);
        setEntitiesIdsAndNonExistEntitiesByTenant(tenant2, modelSummaries2);
        modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 30);
    }

    protected void verifySetEntitiesIdsByTenant() {
        List<ModelSummary> modelSummaries = generateModelSummaries(tenant2.getPid(), 40l, ModelSummaryStatus.ACTIVE);
        setEntitiesIdsByTenant(tenant2, modelSummaries);
        List<ModelSummary> modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 20);

        EntityListCache entityListCache = getEntitiesAndNonExistEntitityIdsByTenant(tenant2);
        assertEquals(entityListCache.getExistEntities().size(), 20);
        assertEquals(entityListCache.getNonExistIds().size(), 10);

        setEntities(modelSummaries);
        modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 30);

        List<String> ids = modelSummaries.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList());
        deleteEntitiesByIds(ids);
        entityListCache = getEntitiesAndNonExistEntitityIdsByTenant(tenant2);
        assertEquals(entityListCache.getExistEntities().size(), 20);
        assertEquals(entityListCache.getNonExistIds().size(), 10);
        deleteIdsByTenantId(tenant2);
        setEntitiesIdsAndNonExistEntitiesByTenant(tenant2, modelSummaries2);
        modelSummaries2 = getEntitiesByTenant(tenant2);
        assertEquals(modelSummaries2.size(), 30);
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testGetEntityById", "testGetEntitiesByIds", "testGetEntitiesByTenant"})
    public void testDeleteIdsByTenantId() {
        deleteIdsByTenantId(tenant1);
        deleteEntitiesByIds(modelSummaryIdsInTenant1);
        assertNull(getEntityById(modelSummaryIdsInTenant1.get(0)));
        assertEquals(getEntitiesByTenant(tenant1).size(), 0);
    }

    @Test(groups = {"functional"}, dependsOnMethods = {"testDeleteIdsByTenantId"})
    public void testDeleteIdsAndEntitiesByTenant() {
        deleteIdsAndEntitiesByTenant(tenant2);
        assertNull(getEntityById(modelSummaryIdsInTenant2.get(0)));
        assertEquals(getEntitiesByTenant(tenant2).size(), 0);
    }

    private void setEntities(List<ModelSummary> modelSummaries) {
        modelSummaryCacheWriter.setEntities(modelSummaries);
    }

    private EntityListCache<ModelSummary> getEntitiesAndNonExistEntitityIdsByTenant(Tenant tenant) {
        EntityListCache<ModelSummary> entityListCache = modelSummaryCacheWriter.getEntitiesAndNonExistEntitityIdsByTenant(tenant);
        return entityListCache;
    }

    private void setEntitiesIdsByTenant(Tenant tenant, List<ModelSummary> modelSummaries) {
        modelSummaryCacheWriter.setEntitiesIdsByTenant(tenant, modelSummaries);
    }

    private void deleteIdsAndEntitiesByTenant(Tenant tenant) {
        modelSummaryCacheWriter.deleteIdsAndEntitiesByTenant(tenant);
    }

    private void setEntitiesIdsAndNonExistEntitiesByTenant(Tenant tenant, List<ModelSummary> allEntities) {
        modelSummaryCacheWriter.setEntitiesIdsAndNonExistEntitiesByTenant(tenant, allEntities);
    }

    private void deleteEntitiesByIds(List<String> ids) {
        modelSummaryCacheWriter.deleteEntitiesByIds(ids);
    }

    private void deleteIdsByTenantId(Tenant tenant) {
        modelSummaryCacheWriter.deleteIdsByTenant(tenant);
    }

    private void setIdsAndEntitiesByTenant(Tenant tenant, List<ModelSummary> modelSummaries) {
        modelSummaryCacheWriter.setIdsAndEntitiesByTenant(tenant, modelSummaries);
    }

    private ModelSummary getEntityById(String id) {
        return modelSummaryCacheWriter.getEntityById(id);
    }

    private List<ModelSummary> getEntitiesByIds(List<String> ids) {
        return modelSummaryCacheWriter.getEntitiesByIds(ids);
    }

    private List<ModelSummary> getEntitiesByTenant(Tenant tenant) {
        return modelSummaryCacheWriter.getEntitiesByTenant(tenant);
    }

    private ModelSummary createModelSummary(long pid, ModelSummaryStatus modelSummaryStatus, long tenantId,
                                            String id) {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setPid(pid);
        modelSummary.setStatus(modelSummaryStatus);
        modelSummary.setDisplayName("displayName");
        modelSummary.setModelType("PythonScriptModel");
        modelSummary.setTenantId(tenantId);
        modelSummary.setId(id);
        return modelSummary;
    }

    private List<ModelSummary> generateModelSummaries(Long tenantId, Long startPid, ModelSummaryStatus
            modelSummaryStatus) {
        List<ModelSummary> result = new ArrayList<>();
        for (long i = startPid; i < startPid + summaryCount; i++) {
            result.add(createModelSummary(i, modelSummaryStatus, tenantId, UUID.randomUUID().toString()));
        }
        return result;
    }

    @DataProvider(name = "modelSummariesByTenant")
    protected Object[][] provideModelSummariesByTenantId() {
        List<ModelSummary> modelSummaries1 = generateModelSummaries(tenant1.getPid(), 0l, ModelSummaryStatus.ACTIVE);
        modelSummaryIdsInTenant1 = modelSummaries1.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList());
        List<ModelSummary> modelSummaries2 = generateModelSummaries(tenant1.getPid(), 10l, ModelSummaryStatus.ACTIVE);
        modelSummaryIdsInTenant1.addAll(modelSummaries2.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList()));
        List<ModelSummary> modelSummaries3 = generateModelSummaries(tenant2.getPid(), 20l, ModelSummaryStatus.INACTIVE);
        modelSummaryIdsInTenant2.addAll(modelSummaries3.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList()));
        List<ModelSummary> modelSummaries4 = generateModelSummaries(tenant2.getPid(), 30l, ModelSummaryStatus.INACTIVE);
        modelSummaryIdsInTenant2.addAll(modelSummaries4.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList()));
        return new Object[][]{
                new Object[]{tenant1, modelSummaries1}, new Object[]{tenant1, modelSummaries2}, new Object[]{tenant2,
                modelSummaries3}, new Object[]{tenant2, modelSummaries4}
        };
    }

    @DataProvider(name = "modelSummaryIds")
    protected Object[][] provideModelSummaryIds() {
        return new Object[][]{new Object[]{modelSummaryIdsInTenant1}, new Object[]{modelSummaryIdsInTenant2}};
    }

    @DataProvider(name = "tenant")
    protected Object[][] provideTenant() {
        return new Object[][]{new Object[]{tenant1}, new Object[]{tenant2}};
    }
}

