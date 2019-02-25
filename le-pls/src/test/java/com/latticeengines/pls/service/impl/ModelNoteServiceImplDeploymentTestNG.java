package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class ModelNoteServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ModelNoteService modelNoteService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    private ModelNote note1;
    private ModelNote note2;

    private ModelSummary modelSummary1;
    private ModelSummary modelSummary2;

    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        testBed.bootstrap(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);

        createModelSummaryForTenant1(tenant1);
        createModelSummaryForTenant2(tenant2);
    }

    private void createModelSummaryForTenant1(Tenant tenant) throws Exception {
        modelSummary1 = new ModelSummary();
        modelSummary1.setId("123");
        modelSummary1.setName("Model1");
        modelSummary1.setApplicationId("application_id_0000");
        modelSummary1.setRocScore(0.75);
        modelSummary1.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        modelSummary1.setTrainingRowCount(8000L);
        modelSummary1.setTestRowCount(2000L);
        modelSummary1.setTotalRowCount(10000L);
        modelSummary1.setTrainingConversionCount(80L);
        modelSummary1.setTestConversionCount(20L);
        modelSummary1.setTotalConversionCount(100L);
        modelSummary1.setConstructionTime(System.currentTimeMillis());
        modelSummary1.setTenant(tenant);
        if (modelSummary1.getConstructionTime() == null) {
            modelSummary1.setConstructionTime(System.currentTimeMillis());
        }
        setDetails(modelSummary1);
        modelSummary1.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummary1.setLastUpdateTime(modelSummary1.getConstructionTime());
        modelSummaryProxy.create(tenant.getId(), modelSummary1);
        modelSummary1 = modelSummaryProxy.getByModelId(modelSummary1.getId());
    }

    private void createModelSummaryForTenant2(Tenant tenant) throws Exception {
        modelSummary2 = new ModelSummary();
        modelSummary2.setId("456");
        modelSummary2.setName("Model2");
        modelSummary2.setRocScore(0.80);
        modelSummary2.setLookupId("TENANT2|Q_EventTable_TENANT2|fghij");
        modelSummary2.setTrainingRowCount(80000L);
        modelSummary2.setTestRowCount(20000L);
        modelSummary2.setTotalRowCount(100000L);
        modelSummary2.setTrainingConversionCount(800L);
        modelSummary2.setTestConversionCount(200L);
        modelSummary2.setTotalConversionCount(1000L);
        modelSummary2.setConstructionTime(System.currentTimeMillis());
        if (modelSummary2.getConstructionTime() == null) {
            modelSummary2.setConstructionTime(System.currentTimeMillis());
        }
        setDetails(modelSummary2);
        modelSummary2.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummary2.setLastUpdateTime(modelSummary2.getConstructionTime());
        modelSummary2.setTenant(tenant);
        modelSummaryProxy.create(tenant.getId(), modelSummary2);
        modelSummary2 = modelSummaryProxy.getByModelId(modelSummary2.getId());
    }

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }

        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
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

    @Test(groups = "deployment")
    public void createModelNoteForSummary1() {
        MultiTenantContext.setTenant(tenant1);
        NoteParams noteParams = new NoteParams();
        noteParams.setUserName("penglong.liu@lattice-engines.com");
        noteParams.setContent("this is a test case");
        modelNoteService.create(modelSummary1.getId(), noteParams);
        List<ModelNote> list = modelNoteService.getAllByModelSummaryId(modelSummary1.getId());
        assertEquals(list.size(), 1);
        note1 = list.get(0);
        assertEquals(note1.getCreatedByUser(), "penglong.liu@lattice-engines.com");
    }

    @Test(groups = "deployment", dependsOnMethods = "createModelNoteForSummary1")
    public void testUpdateByNoteId() {
        NoteParams noteParams1 = new NoteParams();
        noteParams1.setContent("this is not a test case！");
        noteParams1.setUserName("lpl@lattice-engines.com");
        modelNoteService.updateById(note1.getId(), noteParams1);
        List<ModelNote> list1 = modelNoteService.getAllByModelSummaryId(modelSummary1.getId());
        note1 = list1.get(0);
        assertEquals(note1.getLastModifiedByUser(), "lpl@lattice-engines.com");
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateByNoteId")
    public void testCopyNotes() {
        MultiTenantContext.setTenant(tenant2);
        modelNoteService.copyNotes(modelSummary1.getId(), modelSummary2.getId());

        List<ModelNote> list = modelNoteService.getAllByModelSummaryId(modelSummary2.getId());
        assertEquals(list.size(), 1);
        note2 = list.get(0);
        assertEquals(note2.getLastModifiedByUser(), "lpl@lattice-engines.com");
    }

    @Test(groups = "deployment", dependsOnMethods = "testCopyNotes")
    public void testDeleteNotes() {
        modelNoteService.deleteById(note2.getId());
        List<ModelNote> list = modelNoteService.getAllByModelSummaryId(modelSummary2.getId());
        assertEquals(list.size(), 0);
    }
}
