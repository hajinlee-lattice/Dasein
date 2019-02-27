package com.latticeengines.apps.lp.service.impl;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.ModelMetadataService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.security.Tenant;

public class ModelMetadataServiceImplTestNG extends LPFunctionalTestNGBase {

    private static final String TENANT = "PMMLTenant.PMMLTenant.Production";

    @Value("${pls.modelingservice.basedir}")
    private String modelingBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private PmmlModelService pmmlModelService;

    private ModelSummary summary = null;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        CustomerSpace space = CustomerSpace.parse(TENANT);
        HdfsUtils.rmdir(yarnConfiguration, modelingBaseDir + "/" + TENANT);
        HdfsUtils.rmdir(yarnConfiguration, "/Pods/Default/Contracts/" + space.getContractId());

        String tenantLocalPath = ClassLoader.getSystemResource("modelmetadataserviceimpl/" + TENANT).getFile();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, tenantLocalPath, modelingBaseDir);
        String hdfsPathToModelSummary = String.format("%s/%s/models/%s/%s/%s/enhancements/modelsummary.json", //
                modelingBaseDir, //
                TENANT, //
                "PMMLDummyTable-1468539985336", //
                "4f740672-f040-45ba-85be-4801b8042f00", //
                "1468530537792_0050");
        ModelSummaryParser parser = new ModelSummaryParser();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsPathToModelSummary);
        summary = parser.parse(String.format("%s/%s", "4f740672-f040-45ba-85be-4801b8042f00", "1468530537792_0050"), contents);
        Tenant tenant = new Tenant();
        tenant.setId(TENANT);
        tenant.setName(TENANT);
        summary.setTenant(tenant);

        String pivotMappingLocalPath = ClassLoader.getSystemResource("modelmetadataserviceimpl/pivotvalues.csv").getFile();
        String pivotMappingHdfsPath = String.format("/Pods/Default/Contracts/%s/Tenants/%s/Spaces/%s/Metadata/module1/PivotMappings" , //
                space.getContractId(), space.getTenantId(), space.getSpaceId());
        HdfsUtils.mkdir(yarnConfiguration, pivotMappingHdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pivotMappingLocalPath, pivotMappingHdfsPath);
        summary.setPivotArtifactPath(pivotMappingHdfsPath + "/pivotvalues.csv");

        ModelSummaryService mockedModelSummaryService = Mockito.mock(ModelSummaryService.class);
        when(mockedModelSummaryService.findByModelId(anyString(), anyBoolean(), anyBoolean(), anyBoolean())).thenReturn(summary);
        ReflectionTestUtils.setField(modelMetadataService, "modelSummaryService", mockedModelSummaryService);
        ReflectionTestUtils.setField(pmmlModelService, "modelSummaryService", mockedModelSummaryService);
    }

    @Test(groups = "functional")
    public void getRequiredColumnsForPmmlModel() {
        List<Attribute> attrs = modelMetadataService.getRequiredColumns(summary.getId());
        assertEquals(attrs.size(), 100);
    }
}
