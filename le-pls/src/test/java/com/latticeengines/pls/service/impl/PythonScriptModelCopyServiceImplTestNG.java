package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ModelingHdfsUtils;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class PythonScriptModelCopyServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PythonScriptModelCopyServiceImplTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private PythonScriptModelService pythonScriptModelService;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SourceFileService sourceFileService;

    private Tenant modelCopySourceTenant = new Tenant();

    private Tenant modelCopyTargetTenant = new Tenant();

    private SourceFile sourceFile;

    private String sourceFileLocalPath;

    private String outputFileName = "nonLatinInRows.csv";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelCopySourceTenant.setId(CustomerSpace.parse("modelCopySourceTenant").toString());
        modelCopySourceTenant.setName(modelCopySourceTenant.getId());
        modelCopyTargetTenant.setId(CustomerSpace.parse("modelCopyTargetTenant").toString());
        modelCopyTargetTenant.setName(modelCopyTargetTenant.getId());
        try {
            tearDown();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        tenantEntityMgr.create(modelCopySourceTenant);
        tenantEntityMgr.create(modelCopyTargetTenant);

        HdfsUtils.rmdir(yarnConfiguration, customerBase + modelCopySourceTenant.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + modelCopyTargetTenant.getId());
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(modelCopySourceTenant.getId())).toString());
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(modelCopyTargetTenant.getId())).toString());

        String localPathBase = ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel").getPath();
        HdfsUtils.mkdir(yarnConfiguration, customerBase + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase
                + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data",
                customerBase + modelCopySourceTenant.getId());

        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(modelCopySourceTenant.getId())).toString()
                + "/" + outputFileName;
        sourceFileLocalPath = "com/latticeengines/pls/service/impl/modelcopyserviceimpl/pythonscriptmodel/" + outputFileName;
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, ClassLoader.getSystemResourceAsStream(sourceFileLocalPath),
                outputPath);

        setupSecurityContext(modelCopySourceTenant);
        sourceFile = new SourceFile();
        sourceFile.setTenant(modelCopySourceTenant);
        sourceFile.setName(outputFileName);
        sourceFile.setPath(outputPath);
        sourceFile.setTableName("SourceFile_Account_copy_csv");
        sourceFile.setDisplayName(outputFileName);
        sourceFileService.create(sourceFile);
    }

    public void tearDown() throws Exception {
        Tenant sourceTenant = tenantEntityMgr.findByTenantId(modelCopySourceTenant.getId());
        tenantEntityMgr.delete(sourceTenant);
        Tenant targetTenant = tenantEntityMgr.findByTenantId(modelCopyTargetTenant.getId());
        tenantEntityMgr.delete(targetTenant);
    }

    @Test(groups = "functional", enabled = true)
    public void testModelCopyInHdfs() throws IOException {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId("ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
        modelSummary.setDisplayName("some model display name");

        sourceFileService.copySourceFile("cpTrainingTable", sourceFile, modelCopyTargetTenant);
        pythonScriptModelService.copyHdfsData(modelCopySourceTenant.getId(), modelCopyTargetTenant.getId(),
                "AccountModel", "cpTrainingTable", "cpEventTable", modelSummary);
        String path = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration,
                customerBase + modelCopyTargetTenant.getId() + "/models/cpEventTable");
        assertNotNull(path);
        String uuid = UuidUtils.parseUuid(path);

        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);
        JsonNode detail = json.get("ModelDetails");
        assertEquals(detail.get("ModelID").asText(), "ms__" + uuid + "-PLSModel");
        assertEquals(detail.get("LookupID").asText(),
                String.format("%s|%s|%s", modelCopyTargetTenant.getId(), "cpEventTable", uuid));
        assertEquals(detail.get("DisplayName").asText(), "some model display name");
        JsonNode provenance = json.get("EventTableProvenance");
        assertEquals(provenance.get("TrainingTableName").asText(), "cpTrainingTable");
        assertEquals(provenance.get("EventTableName").asText(), "cpEventTable");

        System.out.println(new Path(path).getParent().getParent().toString());
        path = ModelingHdfsUtils.getModelFilePath(yarnConfiguration, new Path(path).getParent().getParent().toString());
        assertNotNull(path);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), "ms__" + uuid + "-PLSModel");

        setupSecurityContext(modelCopyTargetTenant);
        SourceFile newSourceFile = sourceFileService.findByTableName("cpTrainingTable");
        assertNotNull(newSourceFile);
        assertEquals(newSourceFile.getDisplayName(), outputFileName);
        assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, newSourceFile.getPath()),
                FileUtils.readFileToString(new File(ClassLoader.getSystemResource(sourceFileLocalPath).getFile())));
    }
}
