package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.apps.lp.util.ModelingHdfsUtils;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

public class PythonScriptModelCopyServiceImplTestNG extends LPFunctionalTestNGBase {

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

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

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
        String s3TargetCopyTarget = new HdfsToS3PathBuilder()
                .exploreS3FilePath(customerBase + modelCopyTargetTenant.getId() + "/", s3Bucket);
        HdfsUtils.rmdir(yarnConfiguration, s3TargetCopyTarget);

        HdfsUtils.rmdir(yarnConfiguration, PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopySourceTenant.getId()))
                .toString());
        String targetDataDir = PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopyTargetTenant.getId()))
                .toString();
        HdfsUtils.rmdir(yarnConfiguration, targetDataDir);
        String s3TargetDataDir = new HdfsToS3PathBuilder().exploreS3FilePath(targetDataDir, s3Bucket);
        HdfsUtils.rmdir(yarnConfiguration, s3TargetDataDir);

        String localPathBase = ClassLoader.getSystemResource("modelcopyserviceimpl/pythonscriptmodel").getPath();
        HdfsUtils.mkdir(yarnConfiguration, customerBase + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models",
                customerBase + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data",
                customerBase + modelCopySourceTenant.getId());

        String outputPath = PathBuilder
                .buildDataFilePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopySourceTenant.getId()))
                .toString() + "/" + outputFileName;
        sourceFileLocalPath = "modelcopyserviceimpl/pythonscriptmodel/" + outputFileName;
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, ClassLoader.getSystemResourceAsStream(sourceFileLocalPath),
                outputPath);

        MultiTenantContext.setTenant(modelCopySourceTenant);
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

    @SuppressWarnings("deprecation")
    @Test(groups = "functional", enabled = true)
    public void testModelCopyInHdfs() throws IOException {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId("ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
        modelSummary.setDisplayName("some model display name");

        CopySourceFileRequest request = new CopySourceFileRequest();
        request.setOriginalSourceFile(sourceFile.getName());
        request.setTargetTable("cpTrainingTable");
        request.setTargetTenant(modelCopyTargetTenant.getId());
        sourceFileService.copySourceFile(request);
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

        MultiTenantContext.setTenant(modelCopyTargetTenant);
        SourceFile newSourceFile = sourceFileService.findByTableName("cpTrainingTable");
        assertNotNull(newSourceFile);
        assertEquals(newSourceFile.getDisplayName(), outputFileName);
        assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, newSourceFile.getPath()),
                FileUtils.readFileToString(new File(ClassLoader.getSystemResource(sourceFileLocalPath).getFile())));
    }
}
