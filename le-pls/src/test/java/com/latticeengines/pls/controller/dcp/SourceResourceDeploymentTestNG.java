package com.latticeengines.pls.controller.dcp;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class SourceResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String TEST_TEMPLATE_DIR = "le-serviceapps/dcp/deployment/template";
    private static final String TEST_TEMPLATE_NAME = "dcp-accounts-hard-coded.json";
    private static final String TEST_TEMPLATE_VERSION = "1";

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;

    @Inject
    private TestArtifactService testArtifactService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testProjectProxy);
        attachProtectedProxy(testSourceProxy);
    }

    @Test(groups = "deployment")
    public void testCreateAndGetSource() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId("testProject",
                Project.ProjectType.Type1);
        Assert.assertNotNull(projectDetail);
        String projectId = projectDetail.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION, TEST_TEMPLATE_NAME);

        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);

        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setProjectId(projectId);
        sourceRequest.setDisplayName("testSource");
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getImportStatus(), DataFeedTask.S3ImportStatus.Active);
        Assert.assertFalse(StringUtils.isBlank(source.getSourceId()));
        Assert.assertFalse(StringUtils.isBlank(source.getDropFullPath()));

        sourceRequest.setDisplayName("testSource2");
        Source source2 = testSourceProxy.createSource(sourceRequest);

        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());

        testSourceProxy.pauseSourceById(source.getSourceId());
        Source getSource = testSourceProxy.getSource(source.getSourceId());
        Assert.assertNotNull(getSource);
        Assert.assertEquals(getSource.getSourceId(), source.getSourceId());
        Assert.assertEquals(getSource.getImportStatus(), DataFeedTask.S3ImportStatus.Pause);

        List<Source> allSources = testSourceProxy.getSourcesByProject(projectDetail.getProjectId());
        Assert.assertNotNull(allSources);
        Assert.assertEquals(allSources.size(), 2);
        Set<String> allIds = new HashSet<>(Arrays.asList(source.getSourceId(),  source2.getSourceId()));
        allSources.forEach(s -> {
            Assert.assertTrue(allIds.contains(s.getSourceId()));
            Assert.assertFalse(StringUtils.isEmpty(s.getDropFullPath()));
        });

        testSourceProxy.deleteSourceById(source.getSourceId());

        allSources = testSourceProxy.getSourcesByProject(projectDetail.getProjectId());
        Assert.assertEquals(allSources.size(), 1);
        Assert.assertEquals(allSources.get(0).getSourceId(), source2.getSourceId());
    }
}
