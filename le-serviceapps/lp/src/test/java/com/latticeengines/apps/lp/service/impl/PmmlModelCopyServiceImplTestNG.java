package com.latticeengines.apps.lp.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.apps.lp.util.ModelingHdfsUtils;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class PmmlModelCopyServiceImplTestNG extends LPFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PmmlModelCopyServiceImplTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private PmmlModelService pmmlModelService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private Tenant modelCopySourceTenant = new Tenant();

    private Tenant modelCopyTargetTenant = new Tenant();

    private String moduleName = "rfpmml_1474925594307";

    private String pivotFilePath;

    private String localPathBase = ClassLoader.getSystemResource("modelcopyserviceimpl/pmmlmodel").getPath();

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
                .buildMetadataPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopySourceTenant.getId()))
                .toString());
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder
                .buildMetadataPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopyTargetTenant.getId()))
                .toString());

        HdfsUtils.mkdir(yarnConfiguration, PathBuilder
                .buildMetadataPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopySourceTenant.getId()))
                .toString());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/metadata/" + moduleName, PathBuilder
                .buildMetadataPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(modelCopySourceTenant.getId()))
                .toString());

        HdfsUtils.mkdir(yarnConfiguration, customerBase + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/models",
                customerBase + modelCopySourceTenant.getId());
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localPathBase + "/data",
                customerBase + modelCopySourceTenant.getId());

        Module module = new Module();
        module.setName(moduleName);
        Artifact pivotArtifact = new Artifact();
        pivotArtifact.setArtifactType(ArtifactType.PivotMapping);
        pivotArtifact.setTenantId(modelCopySourceTenant.getPid());
        pivotArtifact.setName("pivotvalues");

        pivotFilePath = PathBuilder
                .buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(modelCopySourceTenant.getId()), moduleName, ArtifactType.PivotMapping)
                .toString() + "/" + "pivotvalues.csv";
        pivotArtifact.setPath(pivotFilePath);
        pivotArtifact.setModule(module);

        Artifact pmmlArtifact = new Artifact();
        pmmlArtifact.setArtifactType(ArtifactType.PMML);
        pmmlArtifact.setTenantId(modelCopySourceTenant.getPid());
        pmmlArtifact.setName("rfpmml");
        pmmlArtifact.setPath(PathBuilder
                .buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(modelCopySourceTenant.getId()), moduleName, ArtifactType.PMML)
                .toString() + "/" + "rfpmml.xml");
        pmmlArtifact.setModule(module);
        module.addArtifact(pivotArtifact);
        module.addArtifact(pmmlArtifact);

        MetadataProxy proxy = mock(MetadataProxy.class);

        when(proxy.getModule(modelCopySourceTenant.getId(), moduleName)).thenReturn(module);
        when(proxy.createArtifact(eq(modelCopyTargetTenant.getId()), anyString(), anyString(), any(Artifact.class)))
                .thenReturn(true);
        ReflectionTestUtils.setField(pmmlModelService, "metadataProxy", proxy);

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
        modelSummary.setId("ms__4f1d08f8-3678-420a-b419-8e5dad939834-rfpmml_2");
        modelSummary.setDisplayName("some model display name");
        modelSummary.setModuleName(moduleName);
        modelSummary.setPivotArtifactPath(pivotFilePath);

        MultiTenantContext.setTenant(modelCopySourceTenant);
        pmmlModelService.copyHdfsData(modelCopySourceTenant.getId(), modelCopyTargetTenant.getId(),
                "PMMLDummyTable-1474925639299", "cpTrainingTable", "cpEventTable", modelSummary);
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
        assertTrue(provenance.get("Module_Name").asText().startsWith("cp_module"));
        assertNotEquals(provenance.get(ProvenancePropertyName.PivotFilePath.getName()).asText(), pivotFilePath);
        assertEquals(
                HdfsUtils.getHdfsFileContents(yarnConfiguration,
                        provenance.get(ProvenancePropertyName.PivotFilePath.getName()).asText()),
                FileUtils
                        .readFileToString(
                                new File(ClassLoader
                                        .getSystemResource("modelcopyserviceimpl/pmmlmodel/"
                                                + "metadata/rfpmml_1474925594307/PivotMappings/pivotvalues.csv")
                                        .getFile())));

        System.out.println(new Path(path).getParent().getParent().toString());
        path = ModelingHdfsUtils.getModelFilePath(yarnConfiguration, new Path(path).getParent().getParent().toString());
        assertNotNull(path);
        contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        json = objectMapper.readTree(contents);
        assertEquals(json.get("Summary").get("ModelID").asText(), "ms__" + uuid + "-PLSModel");
    }
}
