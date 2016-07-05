package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class ModelCopyResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(ModelCopyResourceDeploymentTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelCopyService modelCopyService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    private static final String localPathBase = ClassLoader.getSystemResource(
            "com/latticeengines/pls/service/impl/modelcopyserviceimpl").getPath();

    private Tenant tenant1;

    private Tenant tenant2;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        modelSummaryEntityMgr.deleteByModelId("ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
        setupTwoTenants();
        setupHdfs();
        log.info("Wait for 10 seconds to download model summary");
        Thread.sleep(15000L);
        setupTables();
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration,
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId()))
                        .toString());
        HdfsUtils.rmdir(yarnConfiguration,
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant2.getId()))
                        .toString());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant1.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + tenant2.getId());
        modelSummaryEntityMgr.deleteByModelId("ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
    }

    private void setupTwoTenants() throws KeyManagementException, NoSuchAlgorithmException {
        turnOffSslChecking();
        testBed.bootstrap(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);
    }

    private void setupTables() throws IOException {
        Table trainingTable = JsonUtils
                .deserialize(
                        IOUtils.toString(ClassLoader
                                .getSystemResourceAsStream("com/latticeengines/pls/service/impl/modelcopyserviceimpl/tables/TrainingTable.json")),
                        Table.class);
        Extract extract = trainingTable.getExtracts().get(0);
        extract.setPath(PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId()))
                .append("SourceFile_Account_copy_csv").append("Extracts").append("2016-03-31-18-26-19").toString()
                + "/*.avro");
        trainingTable.setExtracts(Arrays.<Extract> asList(new Extract[] { extract }));
        metadataProxy.createTable(tenant1.getId(), trainingTable.getName(), trainingTable);

        Table eventTable = JsonUtils
                .deserialize(
                        IOUtils.toString(ClassLoader
                                .getSystemResourceAsStream("com/latticeengines/pls/service/impl/modelcopyserviceimpl/tables/EventTable.json")),
                        Table.class);
        extract = eventTable.getExtracts().get(0);
        extract.setPath(customerBase + tenant1.getId() + "/data/AccountModel/Samples/allTraining-r-00000.avro");
        eventTable.setExtracts(Arrays.<Extract> asList(new Extract[] { extract }));
        metadataProxy.createTable(tenant1.getId(), eventTable.getName(), eventTable);
    }

    private void setupHdfs() throws IOException {

        HdfsUtils.mkdir(yarnConfiguration, customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data", customerBase + tenant1.getId());
        HdfsUtils.copyFromLocalToHdfs(
                yarnConfiguration, //
                localPathBase + "/data/AccountModel/Samples/allTest-r-00000.avro", //
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant1.getId()))
                        .append("SourceFile_Account_copy_csv").append("Extracts").append("2016-03-31-18-26-19")
                        .append("part1.avro").toString());
    }

    @Test(groups = "deployment")
    public void testModelCopy() throws Exception {
        setupSecurityContext(tenant1);
        modelCopyService.copyModel(tenant2.getId(), "ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");

        log.info("Wait for 10 seconds to download model summary");
        Thread.sleep(15000L);

        setupSecurityContext(tenant2);

        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + tenant2.getId()
                + "/models", new HdfsUtils.HdfsFileFilter() {

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

        paths = HdfsUtils.getFilesForDir(yarnConfiguration, new Path(modelSummaryPath).getParent().getParent()
                .toString(), ".*.model.json");
        assertTrue(paths.size() == 1);
        String modelPath = paths.get(0);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), modelId);

        ModelSummary summary = modelSummaryService.getModelSummaryByModelId(modelId);
        assertNotNull(summary);
        assertEquals(summary.getTrainingTableName(), newTrainingTableName);
        assertEquals(summary.getEventTableName(), newEventTableName);
    }
}
