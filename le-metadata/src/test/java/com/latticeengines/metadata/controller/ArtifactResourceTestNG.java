package com.latticeengines.metadata.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

public class ArtifactResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ArtifactResourceTestNG.class);

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void createArtifact() {

    }

    @Test(groups = "functional")
    public void validateArtifactWithError() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, ClassLoader.getSystemResource(RESOURCE_BASE + "/pivot.csv")
                .getPath(), hdfsPath);
        hdfsPath += "/pivot.csv";
        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/metadata/customerspaces/%s/artifacttype/%s?file=%s", getRestAPIHostPort(),
                        "validateArtifact.validateArtifact.Production", ArtifactType.PivotMapping, hdfsPath), //
                null, ResponseDocument.class);
        assertFalse(response.isSuccess());
        @SuppressWarnings("unchecked")
        List<String> error = new ObjectMapper().convertValue(response.getErrors(), List.class);
        assertEquals(error.get(0),
                "Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType] from the file");
    }

    @Test(groups = "functional")
    public void validateArtifactWithoutError() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        hdfsPath += "/pivotvalues.csv";
        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/metadata/customerspaces/%s/artifacttype/%s?file=%s", getRestAPIHostPort(),
                        "validateArtifact.validateArtifact.Production", ArtifactType.PivotMapping, hdfsPath), //
                null, ResponseDocument.class);
        assertTrue(response.isSuccess());
        String error = new ObjectMapper().convertValue(response.getResult(), String.class);
        assertEquals(error, "");
    }

    @Test(groups = "functional")
    public void getArtifactByPath() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        hdfsPath += "/pivotvalues.csv";

        Tenant t1 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE1);
        Module module = new Module();
        module.setName("M1");
        module.setTenant(t1);

        Artifact pivotMappingFile = new Artifact();
        pivotMappingFile.setArtifactType(ArtifactType.PivotMapping);
        pivotMappingFile.setPath(hdfsPath);
        pivotMappingFile.setName("pivotfile");
        module.addArtifact(pivotMappingFile);

        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        Boolean res = restTemplate.postForObject( //
                String.format("%s/metadata/customerspaces/%s/modules/%s/artifacts/%s", getRestAPIHostPort(),
                        CUSTOMERSPACE1, "module11", "pivotfile"), //
                pivotMappingFile, Boolean.class);
        assertTrue(res);

        Artifact artifact = restTemplate.getForObject( //
                String.format("%s/metadata/customerspaces/%s/artifactpath?file=%s", getRestAPIHostPort(),
                        CUSTOMERSPACE1, hdfsPath), Artifact.class);
        assertEquals(artifact.getPath(), hdfsPath);
    }

}
