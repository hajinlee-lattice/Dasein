package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

public class ModuleResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ArtifactResourceTestNG.class);

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void getModule() throws IOException {
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
                        customerSpace1, ArtifactType.PivotMapping, hdfsPath), //
                null, ResponseDocument.class);
        assertTrue(response.isSuccess());

        Tenant t1 = tenantEntityMgr.findByTenantId(customerSpace1);
        Module module = new Module();
        module.setName("M1");
        module.setTenant(t1);

        Artifact pivotMappingFile = new Artifact();
        pivotMappingFile.setArtifactType(ArtifactType.PivotMapping);
        pivotMappingFile.setPath("/a/b/c");
        pivotMappingFile.setName("pivotfile");
        module.addArtifact(pivotMappingFile);

        Boolean res = restTemplate.postForObject( //
                String.format("%s/metadata/customerspaces/%s/modules/%s/artifacts/%s",
                        getRestAPIHostPort(), customerSpace1, "module11", "pivotfile"), //
                        pivotMappingFile, Boolean.class);
        assertTrue(res);

        Module m = restTemplate.getForObject(String.format("%s/metadata/customerspaces/%s/modules/%s",
                        getRestAPIHostPort(), customerSpace1, "module11"), Module.class);
        System.out.println(m);
        assertEquals(m.getArtifacts().size(), 1);

    }

}
