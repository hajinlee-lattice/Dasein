package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelAlertService;

public class ModelAlertServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String MODEL_ID = "ms__8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a-PLSModel";
    private String tenantId;
    private String dir;

    private String modelSummaryFileHdfsPath;
    private String topPredictorFileHdfsPath;
    private String rfModelFileHdfsPath;
    private String dataDiagnosticsFileHdfsPath;
    private String metadataDiagnosticsFileHdfsPath;

    private URL modelSummaryUrl;
    private URL metadataDiagnosticsUrl;
    private URL dataDiagnosticsUrl;
    private URL rfMoelUrl;
    private URL topPredictorUrl;

    @Autowired
    private ModelAlertService modelAlertService;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();

        tenantId = testingTenants.get(0).getId();
        dir = modelingServiceHdfsBaseDir + "/" + tenantId + "/models/ANY_TABLE/" + MODEL_ID + "/container_01/";
        modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-marketo.json");
        metadataDiagnosticsUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/metadata-diagnostics.json");
        dataDiagnosticsUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/diagnostics.json");
        rfMoelUrl = ClassLoader.getSystemResource("com/latticeengines/pls/functionalframework/rf_model.txt");
        topPredictorUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/topPredictor_model.csv");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");

        modelSummaryFileHdfsPath = dir + "/enhancements/modelsummary.json";
        topPredictorFileHdfsPath = dir + "/topPredictor_model.csv";
        rfModelFileHdfsPath = dir + "/rf_model.txt";
        dataDiagnosticsFileHdfsPath = dir + "/diagnostics.json";
        metadataDiagnosticsFileHdfsPath = dir + "/metadata-diagnostics.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), modelSummaryFileHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataDiagnosticsUrl.getFile(), metadataDiagnosticsFileHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataDiagnosticsUrl.getFile(), dataDiagnosticsFileHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfMoelUrl.getFile(), rfModelFileHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, topPredictorUrl.getFile(), topPredictorFileHdfsPath);

    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + tenantId);
    }

    @Test(groups = { "functional" })
    public void testGenerateMissingMetaDataWarningsDoesNotThrowExceptionWithoutFileInHdfs() throws Exception {
        if (HdfsUtils.fileExists(yarnConfiguration, metadataDiagnosticsFileHdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, metadataDiagnosticsFileHdfsPath);
        }

        ModelAlerts.MissingMetaDataWarnings metadataWarning = null;
        try {
            metadataWarning = modelAlertService.generateMissingMetaDataWarnings(tenantId, MODEL_ID);
        } catch (Exception e) {
            Assert.fail("Should NOT have thrown an exception");
        }
        assertNotNull(metadataWarning);
        assertNull(metadataWarning.getInvalidApprovedUsageAttributes());
        assertNull(metadataWarning.getInvalidCategoryAttributes());
        assertNull(metadataWarning.getInvalidDisplayNameAttributes());
        assertNull(metadataWarning.getInvalidStatisticalTypeAttributes());
        assertNull(metadataWarning.getInvalidTagsAttributes());
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataDiagnosticsUrl.getFile(), metadataDiagnosticsFileHdfsPath);
    }

    @Test(groups = { "functional" })
    public void testGenerateMissingMetaDataWarningsReturnsCorrectWarningWithFileInHdfs() throws Exception {
        if (!HdfsUtils.fileExists(yarnConfiguration, metadataDiagnosticsFileHdfsPath)) {
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataDiagnosticsUrl.getFile(),
                    metadataDiagnosticsFileHdfsPath);
        }

        ModelAlerts.MissingMetaDataWarnings metadataWarning = null;
        try {
            metadataWarning = modelAlertService.generateMissingMetaDataWarnings(tenantId, MODEL_ID);
        } catch (Exception e) {
            Assert.fail("Should NOT have thrown an exception");
        }
        assertNotNull(metadataWarning);
        assertEquals(metadataWarning.getInvalidApprovedUsageAttributes().size(), 2);
        assertEquals(metadataWarning.getInvalidCategoryAttributes().size(), 2);
        assertEquals(metadataWarning.getInvalidDisplayNameAttributes().size(), 2);
        assertEquals(metadataWarning.getInvalidStatisticalTypeAttributes().size(), 2);
        assertEquals(metadataWarning.getInvalidTagsAttributes().size(), 3);
    }

    @Test(groups = { "functional" })
    public void testGenerateModelQualityWarningsReturnsCorrectWarnings() throws Exception {

        ModelAlerts.ModelQualityWarnings modelingWarning = null;
        try {
            modelingWarning = modelAlertService.generateModelQualityWarnings(tenantId, MODEL_ID);
        } catch (Exception e) {
            Assert.fail("Should NOT have thrown an exception");
        }
        System.out.println("modelingWarning is " + modelingWarning);
        assertNotNull(modelingWarning);
        System.out.println("Low success events are: " + modelingWarning.getLowSuccessEvents());
        assertEquals(modelingWarning.getLowSuccessEvents().compareTo(420L), 0);
        assertEquals(modelingWarning.getMinSuccessEvents().compareTo(500L), 0);
        assertEquals(modelingWarning.getMinConversionPercentage().compareTo(5.0D), 0);
        assertEquals(modelingWarning.getLowConversionPercentage().compareTo(4.000762049914269D), 0);
        assertEquals(modelingWarning.getOutOfRangeRocScore().compareTo(0.8896246354710116), 0);
        assertEquals(modelingWarning.getMinRocScore().compareTo(0.7), 0);
        assertEquals(modelingWarning.getMaxRocScore().compareTo(0.85), 0);
        assertEquals(modelingWarning.getExcessiveDiscreteValuesAttributes().size(), 2);
        assertEquals(modelingWarning.getMaxNumberOfDiscreteValues().compareTo(200L), 0);
        assertEquals(modelingWarning.getExcessivePredictiveAttributes().size(), 1);
        assertEquals(modelingWarning.getMaxFeatureImportance().compareTo(0.1D), 0);
        assertEquals(modelingWarning.getExcessivePredictiveNullValuesAttributes().size(), 129);
        assertEquals(modelingWarning.getMaxLiftForNull().compareTo(1.0D), 0);
    }

    @Test(groups = { "functional" })
    public void testGenerateModelQualityWarningsThrowsExceptionWhenFileIsMissing() throws Exception {
        if (HdfsUtils.fileExists(yarnConfiguration, dataDiagnosticsFileHdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, dataDiagnosticsFileHdfsPath);
        }

        ModelAlerts.ModelQualityWarnings modelingWarning = null;
        try {
            modelingWarning = modelAlertService.generateModelQualityWarnings(tenantId, MODEL_ID);
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            assertNull(modelingWarning);
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18042);
        }
    }
}
