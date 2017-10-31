package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelingFileMetadataService;

public class ModelMetadataServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    private static final String TENANT = "PMMLTenant.PMMLTenant.Production";

    private InputStream fileInputStream;

    private File dataFile;
    
    @Value("${pls.modelingservice.basedir}")
    private String modelingBaseDir;
    
    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;
    
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
        
        String tenantLocalPath = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/modelmetadataserviceimpl/" + TENANT).getFile();
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
        
        String pivotMappingLocalPath = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/modelmetadataserviceimpl/pivotvalues.csv").getFile();
        String pivotMappingHdfsPath = String.format("/Pods/Default/Contracts/%s/Tenants/%s/Spaces/%s/Metadata/module1/PivotMappings" , //
                space.getContractId(), space.getTenantId(), space.getSpaceId());
        HdfsUtils.mkdir(yarnConfiguration, pivotMappingHdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pivotMappingLocalPath, pivotMappingHdfsPath);
        summary.setPivotArtifactPath(pivotMappingHdfsPath + "/pivotvalues.csv");
        
        ModelSummaryEntityMgr mockedModelSummaryEntityMgr = Mockito.mock(ModelSummaryEntityMgr.class);
        when(mockedModelSummaryEntityMgr.findByModelId(anyString(), anyBoolean(), anyBoolean(), anyBoolean())).thenReturn(summary);
        ReflectionTestUtils.setField(modelMetadataService, "modelSummaryEntityMgr", mockedModelSummaryEntityMgr);
        ReflectionTestUtils.setField(pmmlModelService, "modelSummaryEntityMgr", mockedModelSummaryEntityMgr);
    }

    @Test(groups = "functional")
    public void uploadFileWithMissingRequiredFields() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
            closeableResourcePool.close();
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertTrue(e.getMessage().contains(InterfaceName.Id.name()));
            assertTrue(e.getMessage().contains(InterfaceName.Event.name()));
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18087);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithEmptyHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;

        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithUnexpectedCharacterInHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_unexpected_character_in_header.csv")
                .getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();

        modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                closeableResourcePool, dataFile.getName());

        closeableResourcePool.close();

    }

    @Test(groups = "functional")
    public void uploadFileWithDuplicateHeaders1() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_duplicate_headers1.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
        } catch (Exception e) {
            thrown = true;
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithDuplicateHeaders2() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_duplicate_headers2.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18107);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void getRequiredColumnsForPmmlModel() throws Exception {
        List<Attribute> attrs = modelMetadataService.getRequiredColumns(summary.getId());
        assertEquals(attrs.size(), 100);
    }
    
}
