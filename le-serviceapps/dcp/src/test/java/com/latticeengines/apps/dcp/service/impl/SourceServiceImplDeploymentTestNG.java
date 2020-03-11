package com.latticeengines.apps.dcp.service.impl;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

public class SourceServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final String SPEC_FILE_LOCAL_PATH = "service/impl/dcp-accounts-example-spec.json";

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxService dropBoxService;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
    }


    @Test(groups = "deployment")
    public void testSourceCreate() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        String projectId = details.getProjectId();

        InputStream specStream = ClassLoader.getSystemResourceAsStream(SPEC_FILE_LOCAL_PATH);

        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId, fieldDefinitionsRecord);

        Assert.assertNotNull(source);

        Assert.assertFalse(StringUtils.isBlank(source.getFullPath()));

        DropBoxSummary dropBoxSummary = dropBoxService.getDropBoxSummary();

        Assert.assertNotNull(dropBoxSummary);
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source.getFullPath());

        // create another source under same project
        Source source2 = sourceService.createSource(mainCustomerSpace, "TestSource2", projectId,
                fieldDefinitionsRecord);
        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getFullPath() + "drop/");
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getFullPath() + "upload/");

    }

}
