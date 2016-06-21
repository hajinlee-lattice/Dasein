package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

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
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.impl.ModelCopyServiceImpl;

public class ModelCopyServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelCopyService modelCopyService;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupMarketoEloquaTestEnvironment();
        String localPathBase = ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/modelcopyserviceimpl").getPath();
        HdfsUtils.mkdir(yarnConfiguration, customerBase + eloquaTenant.getId());
        HdfsUtils
                .copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models", customerBase + eloquaTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data", customerBase + eloquaTenant.getId());
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, customerBase + eloquaTenant.getId());
        HdfsUtils.rmdir(yarnConfiguration, customerBase + marketoTenant.getId());
    }

    @Test(groups = "functional", enabled = true)
    public void testModelCopyInHdfs() throws IOException {
        ((ModelCopyServiceImpl) modelCopyService).processHdfsData(eloquaTenant.getId(), marketoTenant.getId(),
                "ms__20a331e9-f18b-4358-8023-e44a36cb17dd-testWork", "AccountModel", "cpTrainingTable", "cpEventTable");
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + marketoTenant.getId()
                + "/models/cpEventTable", new HdfsUtils.HdfsFileFilter() {

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

        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelSummaryPath);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);
        JsonNode detail = json.get("ModelDetails");
        assertEquals(detail.get("ModelID").asText(), "ms__" + uuid + "-PLSModel");
        assertEquals(detail.get("LookupID").asText(), String.format("%s|%s|%s", marketoTenant.getId(), "cpEventTable", uuid));
        JsonNode provenance = json.get("EventTableProvenance");
        assertEquals(provenance.get("TrainingTableName").asText(), "cpTrainingTable");
        assertEquals(provenance.get("EventTableName").asText(), "cpEventTable");

        paths = HdfsUtils.getFilesForDir(yarnConfiguration, new Path(modelSummaryPath).getParent().getParent()
                .toString(), ".*.model.json");
        assertTrue(paths.size() == 1);
        String modelPath = paths.get(0);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), "ms__" + uuid + "-PLSModel");
    }
}
