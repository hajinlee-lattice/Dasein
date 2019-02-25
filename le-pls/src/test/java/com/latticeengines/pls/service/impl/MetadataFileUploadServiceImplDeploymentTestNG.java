package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class MetadataFileUploadServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        MetadataProxy proxy = mock(MetadataProxy.class);
        when(proxy.createArtifact(anyString(), anyString(), anyString(), any(Artifact.class))).thenReturn(true);
        ReflectionTestUtils.setField(metadataFileUploadService, "metadataProxy", proxy);
        setupMarketoEloquaTestEnvironment();
        switchToSuperAdmin();
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%s", eloquaTenant.getName()));
    }

    @Test(groups = "deployment", dependsOnMethods = { "uploadFile" })
    public void getModules() {
        List<Module> modules = metadataFileUploadService.getModules();
        assertEquals(modules.size(), 3);
    }

    @Test(groups = "deployment", dependsOnMethods = { "uploadFile" })
    public void getArtifacts() {
        List<Artifact> artifacts = metadataFileUploadService.getArtifacts("module2",
                ArtifactType.Function.getUrlToken());
        assertEquals(artifacts.size(), 1);
    }

    @Test(groups = "deployment", dataProvider = "fileScenarios")
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
                new Object[] { ArtifactType.Function.getUrlToken(), //
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
