package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ModelCopyResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(ModelCopyResourceDeploymentTestNG.class);
    private static final String ORIGINAL_MODELID = "ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelCopyService modelCopyService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    private static final String localPathBase = ClassLoader
            .getSystemResource("com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel").getPath();

    private Tenant tenant1;

    private Tenant tenant2;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    private String sourceFileLocalPath;

    private String outputFileName = "nonLatinInRows.csv";

    @Test(groups = "deployment", dataProvider = "dataProvider", timeOut = 900000)
    public void test(boolean scrTenantIsEncrypted, boolean dstTenantIsEncrypted) throws Exception {
        setupTwoTenants(scrTenantIsEncrypted, dstTenantIsEncrypted);
        cleanup();
        setupHdfs();
        setupSecurityContext(tenant1);
        log.info("Wait for 900 seconds to download model summary");
        waitToDownloadModel(ORIGINAL_MODELID);
        setupTables();
        testModelCopy();
        cleanup();
    }

    private void waitToDownloadModel(String modelId) throws InterruptedException {
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
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId())).toString());
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant2.getId())).toString());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant1.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant2.getId());
        ModelSummary model = modelSummaryEntityMgr.getByModelId(ORIGINAL_MODELID);
        if (model != null) {
            modelSummaryEntityMgr.delete(model);
        }
    }

    private void setupTwoTenants(boolean scrTenantIsEncrypted, boolean dstTenantIsEncrypted)
            throws IllegalArgumentException, Exception {
        turnOffSslChecking();
        tenant1 = createTenant(scrTenantIsEncrypted);
        tenant2 = createTenant(dstTenantIsEncrypted);
    }

    private Tenant createTenant(boolean tenantIsEncrypted) throws Exception {
        Tenant tenant = new Tenant();
        if (tenantIsEncrypted) {
            tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.LPA3);
            ConfigurationController<CustomerSpaceScope> controller = ConfigurationController
                    .construct(new CustomerSpaceScope(CustomerSpace.parse(tenant.getId())));
            assertTrue(controller.exists(new com.latticeengines.domain.exposed.camille.Path("/EncryptionKey")));
        } else {
            String TenantName = TestFrameworkUtils.TENANTID_PREFIX + UUID.randomUUID();
            tenant.setName(String.format("%s.%s.Production", TenantName, TenantName));
            tenant.setId(tenant.getName());
            deploymentTestBed.deleteTenant(tenant);
            deploymentTestBed.createTenant(tenant);
        }
        return tenant;
    }

    private void setupTables() throws IOException {
        Table trainingTable = JsonUtils.deserialize(
                IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                        "com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel/tables/TrainingTable.json")),
                Table.class);
        Extract extract = trainingTable.getExtracts().get(0);
        extract.setPath(
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId()))
                        .append("SourceFile_Account_copy_csv").append("Extracts").append("2016-03-31-18-26-19")
                        .toString() + "/*.avro");
        trainingTable.setExtracts(Arrays.<Extract> asList(new Extract[] { extract }));
        metadataProxy.createTable(tenant1.getId(), trainingTable.getName(), trainingTable);

        Table eventTable = JsonUtils.deserialize(
                IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                        "com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel/tables/EventTable.json")),
                Table.class);
        extract = eventTable.getExtracts().get(0);
        extract.setPath(customerBase + tenant1.getId() + "/data/AccountModel/Samples/allTraining-r-00000.avro");
        eventTable.setExtracts(Arrays.<Extract> asList(new Extract[] { extract }));
        metadataProxy.createTable(tenant1.getId(), eventTable.getName(), eventTable);

        String outputPath = PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId())).toString() + "/"
                + outputFileName;
        sourceFileLocalPath = "com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel/"
                + outputFileName;
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, ClassLoader.getSystemResourceAsStream(sourceFileLocalPath),
                outputPath);
        SourceFile sourceFile = new SourceFile();
        sourceFile.setTenant(tenant1);
        sourceFile.setName(outputFileName);
        sourceFile.setPath(outputPath);
        sourceFile.setTableName(trainingTable.getName());
        sourceFile.setDisplayName(outputFileName);
        sourceFileEntityMgr.create(sourceFile);
    }

    private void setupHdfs() throws IOException {
        HdfsUtils.mkdir(yarnConfiguration, customerBase + tenant1.getId());
        System.out.println(localPathBase + "/models");
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data", customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, //
                localPathBase + "/data/AccountModel/Samples/allTest-r-00000.avro", //
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId()))
                        .append("SourceFile_Account_copy_csv").append("Extracts").append("2016-03-31-18-26-19")
                        .append("part1.avro").toString());
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(tenant1.getId());
    }

    private void testModelCopy() throws Exception {

        modelCopyService.copyModel(tenant2.getId(), ORIGINAL_MODELID);

        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(tenant2.getId());

        setupSecurityContext(tenant2);

        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                customerBase + tenant2.getId() + "/models", new HdfsUtils.HdfsFileFilter() {

                    @Override
                    public boolean accept(FileStatus file) {
                        if (file == null) {
                            return false;
                        }
                        String name = file.getPath().getName().toString();
                        return name.equals("modelsummary.json");
                    }
                });
        assertTrue(paths.size() == 1);
        String modelSummaryPath = paths.get(0);
        String uuid = UuidUtils.parseUuid(modelSummaryPath);
        String modelId = "ms__" + uuid + "-PLSModel";

        List<Table> tables = metadataProxy.getTables(tenant2.getId());
        assertTrue(tables.size() == 2);
        String newEventTableName = modelSummaryPath.split("/")[8];
        String newTrainingTableName = tables.get(0).getName().equals(newEventTableName) ? tables.get(1).getName()
                : tables.get(0).getName();

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
                sourceFileEntityMgr.getByTableName(newTrainingTableName).getPath());

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

        SourceFile newSourceFile = sourceFileEntityMgr.getByTableName(newTrainingTableName);
        assertNotNull(newSourceFile);
        assertEquals(newSourceFile.getDisplayName(), outputFileName);
        assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, newSourceFile.getPath()),
                FileUtils.readFileToString(new File(ClassLoader.getSystemResource(sourceFileLocalPath).getFile())));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { { Boolean.FALSE, Boolean.TRUE }, { Boolean.TRUE, Boolean.FALSE } };
    }
}
