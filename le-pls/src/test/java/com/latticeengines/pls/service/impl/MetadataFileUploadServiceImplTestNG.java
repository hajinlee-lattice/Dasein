package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.MetadataFileUploadService;

public class MetadataFileUploadServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private MetadataFileUploadService metadataFileUploadService;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant1", contractId));
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant2", contractId));
        setupMarketoEloquaTestEnvironment();
        switchToSuperAdmin();
    }

    @Test(groups = "functional", dependsOnMethods = { "uploadFile" })
    public void getModules() {
        List<Module> modules = metadataFileUploadService.getModules();
        assertEquals(modules.size(), 3);
    }

    @Test(groups = "functional", dependsOnMethods = { "uploadFile" })
    public void getArtifacts() {
        List<Artifact> artifacts = metadataFileUploadService.getArtifacts("module2",
                ArtifactType.PythonModule.getUrlToken());
        assertEquals(artifacts.size(), 1);
    }

    @Test(groups = "functional", dataProvider = "fileScenarios")
    public void uploadFile(String artifactType, //
            String moduleName, //
            String artifactName, //
            InputStream inputStream, //
            boolean exception, //
            String expectedPathSuffix) throws Exception {
        boolean exceptionThrown = false;
        try {
            String result = metadataFileUploadService.uploadFile(artifactType, moduleName, artifactName, inputStream);
            assertTrue(result.endsWith(expectedPathSuffix));
        } catch (Exception e) {
            exceptionThrown = true;
            assertEquals(e.getMessage(), expectedPathSuffix);
        }
        assertEquals(exceptionThrown, exception);
    }
    
    @DataProvider(name = "fileScenarios") 
    public Object[][] getFileScenarios() {
        return new Object[][] {
                new Object[] { ArtifactType.PMML.getUrlToken(), //
                        "module1", //
                        "pmml1", //
                        getInputStreamFromResource("rfpmml.xml"), //
                        false, //
                        "/Metadata/module1/PMMLFiles/pmml1.xml" // 
                },
                new Object[] { ArtifactType.PMML.getUrlToken(), //
                        "module1", //
                        "pmml1", //
                        getInputStreamFromResource("rfpmml.xml"), //
                        true, //
                        LedpException.buildMessage(LedpCode.LEDP_18091, new String[] { ArtifactType.PMML.getCode(), "pmml1", "module1" })
                },
                new Object[] { "unknowntype", null, null, null, true, //
                        LedpException.buildMessage(LedpCode.LEDP_18090, new String[] { "unknowntype" })
                },
                new Object[] { ArtifactType.PythonModule.getUrlToken(), //
                        "module2", //
                        "transform1", //
                        getInputStreamFromResource("a.py"), //
                        false, //
                        "/Metadata/module2/PythonModules/transform1.py" // 
                },
                new Object[] { ArtifactType.PivotMapping.getUrlToken(), //
                        "module3", //
                        "sfdc_pmml_mapping", //
                        getInputStreamFromResource("sfdc_pmml_mapping.txt"), //
                        false, //
                        "/Metadata/module3/PivotMappings/sfdc_pmml_mapping.csv" // 
                },
        };
    }
    
    private InputStream getInputStreamFromResource(String fileName) {
        return ClassLoader.getSystemResourceAsStream("com/latticeengines/pls/service/impl/metadatafileuploadserviceimpl/" + fileName);
    }
}
