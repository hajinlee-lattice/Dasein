package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelReplaceService;
import com.latticeengines.pls.util.ModelingHdfsUtils;

public class ModelReplaceServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelReplaceService modelReplaceService;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    private Tenant modelReplaceSourceTenant = new Tenant();

    private Tenant modelReplaceTargetTenant = new Tenant();

    private ModelSummary sourceModelSummary;

    private ModelSummary targetModelSummary;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelReplaceSourceTenant.setId("modelReplaceSourceTenant");
        modelReplaceTargetTenant.setId("modelReplaceTargetTenant");

        HdfsUtils.rmdir(yarnConfiguration, customerBase + modelReplaceSourceTenant.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + modelReplaceTargetTenant.getId());

        String localPathBase = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/modelreplaceserviceimpl").getPath();
        HdfsUtils.mkdir(yarnConfiguration, customerBase + modelReplaceSourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase
                + modelReplaceSourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data", customerBase
                + modelReplaceSourceTenant.getId());

        localPathBase = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/modelcopyserviceimpl")
                .getPath();
        HdfsUtils.mkdir(yarnConfiguration, customerBase + modelReplaceTargetTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase
                + modelReplaceTargetTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data", customerBase
                + modelReplaceTargetTenant.getId());

        String sourceModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, customerBase + "/"
                + modelReplaceSourceTenant.getId());
        String targetModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, customerBase + "/"
                + modelReplaceTargetTenant.getId());

        sourceModelSummary = modelSummaryParser.parse(sourceModelSummaryPath,
                HdfsUtils.getHdfsFileContents(yarnConfiguration, sourceModelSummaryPath));
        targetModelSummary = modelSummaryParser.parse(targetModelSummaryPath,
                HdfsUtils.getHdfsFileContents(yarnConfiguration, targetModelSummaryPath));
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
    }

    @Test(groups = "functional", enabled = true)
    public void testModelReplaceInHdfs() throws IOException {
        ((ModelReplaceServiceImpl) modelReplaceService).processHdfsData(modelReplaceSourceTenant.getId(),
                modelReplaceTargetTenant.getId(), sourceModelSummary, targetModelSummary);

        String modelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, customerBase + "/"
                + modelReplaceTargetTenant.getId());

        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelSummaryPath);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);
        JsonNode detail = json.get("ModelDetails");
        assertEquals(detail.get("ModelID").asText(), "ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
        assertEquals(
                detail.get("LookupID").asText(),
                "DemoContract.DemoTenant.Production|RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib|20a331e9-f18b-4358-8023-e44a36cb17d1");

        JsonNode provenance = json.get("EventTableProvenance");
        assertEquals(provenance.get("Event_Table_Name").asText(),
                "DataCloudMatchEvent_with_std_attrib_With_UserRefinedAttributes");
        assertEquals(provenance.get("Training_Table_Name").asText(), "clone_86232ca6_67eb_427c_81db_05f74bea74ea");
        assertEquals(provenance.get("EventTableName").asText(), "AccountModel");
        assertEquals(provenance.get("TrainingTableName").asText(), "SourceFile_Account_copy_csv");

        System.out.println(new Path(modelSummaryPath).getParent().getParent().toString());
        List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, new Path(modelSummaryPath).getParent()
                .getParent().toString(), ".*.model.json");
        assertTrue(paths.size() == 1);
        String modelPath = paths.get(0);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), "ms__20a331e9-f18b-4358-8023-e44a36cb17d1-testWork");
    }
}
