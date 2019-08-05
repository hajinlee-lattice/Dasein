package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.HttpServerErrorException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

    private static final Logger log = LoggerFactory.getLogger(ArtifactResourceTestNG.class);

    @Override
    @BeforeClass(groups = "functional", enabled = false)
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", enabled = false)
    public void createArtifact() {

    }

    @Test(groups = "functional", enabled = false)
    public void validateArtifactWithError() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivot.csv").getPath(), hdfsPath);
        hdfsPath += "/pivot.csv";
        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        try {
            restTemplate.postForObject( //
                    String.format("%s/metadata/customerspaces/%s/artifacttype/%s?file=%s", getRestAPIHostPort(),
                            "validateArtifact.validateArtifact.Production", ArtifactType.PivotMapping, hdfsPath), //
                    null, ResponseDocument.class);
            assertTrue(false);
        } catch (HttpServerErrorException e) {
            log.error(ExceptionUtils.getStackTrace(e));
            log.warn("ResponseBodyAsString is " + e.getResponseBodyAsString());
            assertTrue(e.getResponseBodyAsString()
                    .contains("Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType]"));
        }
    }

    @Test(groups = "functional", enabled = false)
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

    @Test(groups = "functional", enabled = false)
    public void getArtifactByPath() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        hdfsPath += "/pivotvalues.csv";

        Tenant t1 = tenantEntityMgr.findByTenantId(customerSpace1);
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
                        customerSpace1, "module11", "pivotfile"), //
                pivotMappingFile, Boolean.class);
        assertTrue(res);

        Artifact artifact = restTemplate.getForObject( //
                String.format("%s/metadata/customerspaces/%s/artifactpath?file=%s", getRestAPIHostPort(),
                        customerSpace1, hdfsPath),
                Artifact.class);
        assertEquals(artifact.getPath(), hdfsPath);
    }

}
