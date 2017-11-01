package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;

public class ArtifactResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ArtifactResourceDeploymentTestNG.class);

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    private MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Autowired
    private TenantService tenantService;

    @Value("${common.test.microservice.url}")
    private String hostPort;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();

        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
    }

    @Test(groups = "deployment")
    public void validateArtifactWithError() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivot.csv").getPath(), hdfsPath);
        hdfsPath += "/pivot.csv";

        try {
            restTemplate.postForObject( //
                    String.format("%s/metadata/customerspaces/%s/artifacttype/%s?file=%s", getRestAPIHostPort(),
                            "validateArtifact.validateArtifact.Production", ArtifactType.PivotMapping, hdfsPath), //
                    null, ResponseDocument.class);
            assertTrue(false);
        } catch (HttpServerErrorException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            log.warn("ResponseBodyAsString is " + e.getResponseBodyAsString());
            assertTrue(e.getResponseBodyAsString()
                    .contains("Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType]"));
        }
    }

    @Test(groups = "deployment")
    public void validateArtifactWithoutError() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        hdfsPath += "/pivotvalues.csv";

        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/metadata/customerspaces/%s/artifacttype/%s?file=%s", getRestAPIHostPort(),
                        "validateArtifact.validateArtifact.Production", ArtifactType.PivotMapping, hdfsPath), //
                null, ResponseDocument.class);
        assertTrue(response.isSuccess());
        String error = new ObjectMapper().convertValue(response.getResult(), String.class);
        assertEquals(error, "");
    }

    @Test(groups = "deployment")
    public void getArtifactByPath() throws IOException {
        String hdfsPath = "/tmp/artifact";
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        hdfsPath += "/pivotvalues.csv";

        Tenant t1 = tenantService.findByTenantId(customerSpace1);
        Module module = new Module();
        module.setName("M1");
        module.setTenant(t1);

        Artifact pivotMappingFile = new Artifact();
        pivotMappingFile.setArtifactType(ArtifactType.PivotMapping);
        pivotMappingFile.setPath(hdfsPath);
        pivotMappingFile.setName("pivotfile");
        module.addArtifact(pivotMappingFile);

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

    private String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }
}
