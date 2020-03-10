package com.latticeengines.pls.controller.dcp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;

public class SourceResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testProjectProxy);
        attachProtectedProxy(testSourceProxy);
    }

    @Test(groups = "deployment", enabled = false)
    public void testCreateAndGetSource() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId("testProject",
                Project.ProjectType.Type1);
        Assert.assertNotNull(projectDetail);
        String projectId = projectDetail.getProjectId();
        SimpleTemplateMetadata simpleTemplateMetadata = new SimpleTemplateMetadata();
        simpleTemplateMetadata.setEntityType(EntityType.Accounts);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setProjectId(projectId);
        sourceRequest.setDisplayName("testSource");
        sourceRequest.setSimpleTemplateMetadata(simpleTemplateMetadata);
        Source source = testSourceProxy.createSource(sourceRequest);
        Assert.assertNotNull(source);
        Assert.assertFalse(StringUtils.isBlank(source.getSourceId()));
        Assert.assertFalse(StringUtils.isBlank(source.getFullPath()));

        sourceRequest.setDisplayName("testSource2");
        Source source2 = testSourceProxy.createSource(sourceRequest);

        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());

        Source getSource = testSourceProxy.getSource(source.getSourceId());
        Assert.assertNotNull(getSource);
        Assert.assertEquals(getSource.getSourceId(), source.getSourceId());

        List<Source> allSources = testSourceProxy.getSourcesByProject(projectDetail.getProjectId());
        Assert.assertNotNull(allSources);
        Assert.assertEquals(allSources.size(), 2);
        Set<String> allIds = new HashSet<>(Arrays.asList(source.getSourceId(),  source2.getSourceId()));
        allSources.forEach(s -> Assert.assertTrue(allIds.contains(s.getSourceId())));

        testSourceProxy.deleteSourceById(source.getSourceId());

        allSources = testSourceProxy.getSourcesByProject(projectDetail.getProjectId());
        Assert.assertEquals(allSources.size(), 1);
        Assert.assertEquals(allSources.get(0).getSourceId(), source2.getSourceId());
    }
}
