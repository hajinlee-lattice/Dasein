package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.service.ModelCopyService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ModelCopyServiceDeploymentTestNG extends LPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory
            .getLogger(ModelCopyServiceDeploymentTestNG.class);
    private static final String ORIGINAL_MODELID = "ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork";
    private static final String S3N_TMP_DIR_KEY = "fs.s3.buffer.dir";
    private static final String S3A_TMP_DIR_KEY = "fs.s3a.buffer.dir";
    private static final String S3_CONNECTOR_TMP_DIR_VALUE = Paths.get("./s3_buffer_tmp").toAbsolutePath().normalize()
            .toString();

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelCopyService modelCopyService;

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    private static final String localPathBase = ClassLoader
            .getSystemResource("modelcopyserviceimpl/pythonscriptmodel").getPath();

    private Tenant tenant1;

    private Tenant tenant2;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    private String sourceFileLocalPath;

    private String outputFileName = "nonLatinInRows.csv";

    @Test(groups = "deployment", dataProvider = "dataProvider", timeOut = 2700000)
    public void test(boolean scrTenantIsEncrypted, boolean dstTenantIsEncrypted) throws Exception {
        setupTwoTenants(scrTenantIsEncrypted, dstTenantIsEncrypted);
        try {
            Pair<String, String> s3ConnectorTmpPath = setupTmpDir();
            cleanup();
            setupHdfs();
            MultiTenantContext.setTenant(tenant1);
            log.info("Wait for 900 seconds to download model summary");
            waitToDownloadModel(ORIGINAL_MODELID);
            setupTables();
            testModelCopy();
            cleanup();
            cleanupTmpDir(s3ConnectorTmpPath);
        } catch (Exception e) {
            log.error("Unexpected exception happened:", e);
        }
    }

    /*
     * set tmp directory for s3 connector and make sure the directory exists
     */
    private Pair<String, String> setupTmpDir() throws Exception {
        File tmpDir = new File(S3_CONNECTOR_TMP_DIR_VALUE);
        FileUtils.deleteQuietly(tmpDir);
        FileUtils.forceMkdir(tmpDir);
        // perserve the old value so that we can recover
        String s3nOldValue = yarnConfiguration.get(S3N_TMP_DIR_KEY);
        String s3aOldValue = yarnConfiguration.get(S3A_TMP_DIR_KEY);
        yarnConfiguration.set(S3N_TMP_DIR_KEY, S3_CONNECTOR_TMP_DIR_VALUE);
        yarnConfiguration.set(S3A_TMP_DIR_KEY, S3_CONNECTOR_TMP_DIR_VALUE);
        log.info("Setting tmp directory for s3 connector to {}", S3_CONNECTOR_TMP_DIR_VALUE);
        return Pair.of(s3nOldValue, s3aOldValue);
    }

    /*
     * restore tmp directory path for s3n and cleanup
     */
    private void cleanupTmpDir(Pair<String, String> oldPaths) throws Exception {
        log.info("Restoring tmp directory for s3n connector back to {}, s3a back to {}", oldPaths.getLeft(),
                oldPaths.getRight());
        yarnConfiguration.set(S3N_TMP_DIR_KEY, oldPaths.getLeft());
        yarnConfiguration.set(S3A_TMP_DIR_KEY, oldPaths.getRight());
        FileUtils.deleteQuietly(new File(S3_CONNECTOR_TMP_DIR_VALUE));
    }

    private void waitToDownloadModel(String modelId) throws InterruptedException {
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        modelSummaryService.downloadModelSummary(tenant1.getId(), modelApplicationIdToEventColumn);
        while (true) {
            ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
            if (modelSummary == null) {
                Thread.sleep(1000L);
                log.info("model is null");
            } else {
                break;
            }
        }
    }

    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration,
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(tenant1.getId())).toString());
        HdfsUtils.rmdir(yarnConfiguration,
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(tenant2.getId())).toString());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant1.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant2.getId());

        ModelSummary model = modelSummaryService.getModelSummaryByModelId(ORIGINAL_MODELID);
        if (model != null) {
            modelSummaryService.delete(model);
        }
    }

    private void setupTwoTenants(boolean scrTenantIsEncrypted, boolean dstTenantIsEncrypted) {
        SSLUtils.turnOffSSLNameVerification();
        tenant1 = createTenant(scrTenantIsEncrypted);
        tenant2 = createTenant(dstTenantIsEncrypted);
    }

    private Tenant createTenant(boolean tenantIsEncrypted) {
        Tenant tenant = new Tenant();
        if (tenantIsEncrypted) {
            tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.LPA3);
        } else {
            String TenantName = TestFrameworkUtils.generateTenantName();
            tenant.setName(String.format("%s.%s.Production", TenantName, TenantName));
            tenant.setId(tenant.getName());
            deploymentTestBed.deleteTenant(tenant);
            deploymentTestBed.createTenant(tenant);
        }
        tenant = deploymentTestBed.getTenantBasedOnId(tenant.getId());
        return tenant;
    }

    private void setupTables() throws IOException {
        Table trainingTable = JsonUtils
                .deserialize(
                        IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                                getResourcePath("tables/TrainingTable.json")), "UTF-8"),
                        Table.class);
        Extract extract = trainingTable.getExtracts().get(0);
        extract.setPath(PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(tenant1.getId()))
                .append("SourceFile_Account_copy_csv").append("Extracts")
                .append("2016-03-31-18-26-19").toString() + "/*.avro");
        trainingTable.setExtracts(Collections.singletonList(extract));
        metadataProxy.createTable(tenant1.getId(), trainingTable.getName(), trainingTable);

        Table eventTable = JsonUtils.deserialize(IOUtils.toString(
                ClassLoader.getSystemResourceAsStream(getResourcePath("tables/EventTable.json")),
                "UTF-8"), Table.class);
        extract = eventTable.getExtracts().get(0);
        extract.setPath(customerBase + tenant1.getId()
                + "/data/AccountModel/Samples/allTraining-r-00000.avro");
        eventTable.setExtracts(Collections.singletonList(extract));
        metadataProxy.createTable(tenant1.getId(), eventTable.getName(), eventTable);

        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(tenant1.getId())).toString() + "/" + outputFileName;
        sourceFileLocalPath = getResourcePath(outputFileName);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration,
                ClassLoader.getSystemResourceAsStream(sourceFileLocalPath), outputPath);
        SourceFile sourceFile = new SourceFile();
        sourceFile.setTenant(tenant1);
        sourceFile.setName(outputFileName);
        sourceFile.setPath(outputPath);
        sourceFile.setTableName(trainingTable.getName());
        sourceFile.setDisplayName(outputFileName);
        sourceFileService.create(sourceFile);
    }

    private void setupHdfs() throws IOException {
        HdfsUtils.mkdir(yarnConfiguration, customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models",
                customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data",
                customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, //
                localPathBase + "/data/AccountModel/Samples/allTest-r-00000.avro", //
                PathBuilder
                        .buildDataFilePath(CamilleEnvironment.getPodId(),
                                CustomerSpace.parse(tenant1.getId()))
                        .append("SourceFile_Account_copy_csv").append("Extracts")
                        .append("2016-03-31-18-26-19").append("part1.avro").toString());
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(tenant1.getId());
    }

    private void testModelCopy() throws Exception {
        modelCopyService.copyModel(tenant2.getId(), ORIGINAL_MODELID);

        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(tenant2.getId());

        setupSecurityContext(tenant2);

        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                customerBase + tenant2.getId() + "/models", file -> {
                    if (file == null) {
                        return false;
                    }
                    String name = file.getPath().getName();
                    return name.equals("modelsummary.json");
                });
        assertTrue(paths.size() == 1);
        String modelSummaryPath = paths.get(0);
        String uuid = UuidUtils.parseUuid(modelSummaryPath);
        String modelId = "ms__" + uuid + "-PLSModel";

        List<Table> tables = metadataProxy.getTables(tenant2.getId());
        assertTrue(tables.size() == 2);
        String newEventTableName = modelSummaryPath.split("/")[8];
        String newTrainingTableName = tables.get(0).getName().equals(newEventTableName)
                ? tables.get(1).getName() : tables.get(0).getName();

        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelSummaryPath);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);
        JsonNode detail = json.get("ModelDetails");
        assertEquals(detail.get("ModelID").asText(), modelId);
        assertEquals(detail.get("LookupID").asText(),
                String.format("%s|%s|%s", tenant2.getId(), newEventTableName, uuid));
        JsonNode provenance = json.get("EventTableProvenance");
        assertEquals(provenance.get("TrainingTableName").asText(), newTrainingTableName);
        assertEquals(provenance.get("EventTableName").asText(), newEventTableName);
        assertEquals(provenance.get(ProvenancePropertyName.TrainingFilePath.getName()).asText(),
                sourceFileService.getByTableNameCrossTenant(newTrainingTableName).getPath());

        paths = HdfsUtils.getFilesForDir(yarnConfiguration,
                new Path(modelSummaryPath).getParent().getParent().toString(), ".*.model.json");
        assertTrue(paths.size() == 1);
        String modelPath = paths.get(0);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), modelId);

        waitToDownloadModel(modelId);

        ModelSummary summary = modelSummaryService.getModelSummaryByModelId(modelId);
        assertNotNull(summary);
        assertEquals(summary.getTrainingTableName(), newTrainingTableName);
        assertEquals(summary.getEventTableName(), newEventTableName);

        SourceFile newSourceFile = sourceFileService
                .getByTableNameCrossTenant(newTrainingTableName);
        assertNotNull(newSourceFile);
        assertEquals(newSourceFile.getDisplayName(), outputFileName);
        assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, newSourceFile.getPath()),
                FileUtils.readFileToString(
                        new File(ClassLoader.getSystemResource(sourceFileLocalPath).getFile()),
                        "UTF-8"));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { { Boolean.FALSE, Boolean.TRUE }, { Boolean.TRUE, Boolean.FALSE } };
    }

    private String getResourcePath(String relativePath) {
        return "modelcopyserviceimpl/pythonscriptmodel/" + relativePath;
    }
}
