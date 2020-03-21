package com.latticeengines.pls.end2end.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.util.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class ProjectSourceUploadDeploymentTestNG extends DCPDeploymentTestNGBase {


    private static final String PROJECT_NAME = "testProjectName";

    private static final String PROJECT_ID = "testProjectId";

    private static final String SOURCE_NAME = "testSourceName";

    private static final String SOURCE_ID = "SourceId";

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;


    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testFlow() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName(PROJECT_NAME);
        projectRequest.setProjectId(PROJECT_ID);
        projectRequest.setProjectType(Project.ProjectType.Type1);
        ProjectDetails details = projectService.createProject(customerSpace, projectRequest,
                MultiTenantContext.getEmailAddress());
        Assert.assertEquals(PROJECT_NAME, details.getProjectDisplayName());

        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName(SOURCE_NAME);
        sourceRequest.setProjectId(PROJECT_ID);
        SimpleTemplateMetadata simpleTemplateMetadata = new SimpleTemplateMetadata();
        simpleTemplateMetadata.setEntityType(EntityType.Accounts);
        sourceRequest.setSimpleTemplateMetadata(simpleTemplateMetadata);
        Source accountSource = sourceService.createSource(sourceRequest);
        Assert.assertNotNull(accountSource);

        // create another source with id under same project
        sourceRequest.setSourceId(SOURCE_ID);
        simpleTemplateMetadata.setEntityType(EntityType.Contacts);
        Source contactSource = sourceService.createSource(sourceRequest);
        Assert.assertNotNull(contactSource);
        
        // TODO(penglong) drop file to s3 to trigger flow




    }

    @Test(groups = "deployment", dependsOnMethods = "testFlow")
    public void testGetAndDelete() {
        List<Project> projects = projectService.getAllProjects(customerSpace);
        Assert.assertNotNull(projects);
        Assert.assertEquals(projects.size(), 1);
        ProjectDetails details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        Assert.assertFalse(details.getDeleted());
        Assert.assertEquals(details.getProjectId(), PROJECT_ID);
        Assert.assertEquals(details.getProjectDisplayName(), PROJECT_NAME);

        List<Source> sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 2);
        List<Source> sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertEquals(sources2.size(), 2);

        // delete one source
        sourceService.deleteSource(SOURCE_ID);
        details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        Source source = sources.get(0);

        // soft delete project
        projectService.deleteProject(customerSpace, PROJECT_ID);
        projects = projectService.getAllProjects(customerSpace);
        Assert.assertFalse(CollectionUtils.isEmpty(projects));
        details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        Assert.assertTrue(details.getDeleted());

        // check source
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        source = sourceService.getSource(source.getSourceId());
        Assert.assertNotNull(source);

        // delete source
        sourceService.deleteSource(source.getSourceId());
        Assert.assertTrue(CollectionUtils.isEmpty(sourceService.getSourceList(PROJECT_ID)));
    }


}
