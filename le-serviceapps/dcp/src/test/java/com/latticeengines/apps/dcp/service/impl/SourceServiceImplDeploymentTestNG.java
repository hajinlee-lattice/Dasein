package com.latticeengines.apps.dcp.service.impl;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class SourceServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private MatchRuleService matchRuleService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
    }


    @Test(groups = "deployment")
    public void testSourceCreate() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com", getPurposeOfUse(), null);
        String projectId = details.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION, TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId,
                fieldDefinitionsRecord);

        Assert.assertNotNull(source);

        Assert.assertFalse(StringUtils.isBlank(source.getDropFullPath()));

        List<S3ImportSystem> allSystems = cdlProxy.getS3ImportSystemList(mainCustomerSpace);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allSystems));
        Optional<S3ImportSystem> dcpSystem =
                allSystems.stream().filter(importSystem -> S3ImportSystem.SystemType.DCP.equals(importSystem.getSystemType())).findFirst();
        Assert.assertTrue(dcpSystem.isPresent());
        List<DataFeedTask> dataFeedTasks = dcpSystem.get().getTasks();
        Assert.assertEquals(dataFeedTasks.size(), 1);
        Table template = dataFeedTasks.get(0).getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.CompanyName.name()),
                "getAttribute on CompanyName returned mull");

        DropBoxSummary dropBoxSummary = dropBoxService.getDropBoxSummary();

        Assert.assertNotNull(dropBoxSummary);
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source.getRelativePathUnderDropfolder());

        // check existence/correctness of base match rule
        List<MatchRule> matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, source.getSourceId(), Boolean.FALSE, Boolean.FALSE);
        Assert.assertEquals(matchRules.size(), 1);
        MatchRule baseRule = matchRules.get(0);
        Assert.assertEquals(baseRule.getSourceId(), source.getSourceId());
        Assert.assertEquals(baseRule.getRuleType(), MatchRuleRecord.RuleType.BASE_RULE);
        Assert.assertEquals(baseRule.getAcceptCriterion().getLowestConfidenceCode(), 6);
        Assert.assertEquals(baseRule.getAcceptCriterion().getHighestConfidenceCode(), 10);

        // create another source under same project
        Source source2 = sourceService.createSource(mainCustomerSpace, "TestSource2", projectId,
                fieldDefinitionsRecord);
        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getRelativePathUnderDropfolder() + "drop/");
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getRelativePathUnderDropfolder() + "Uploads/");

        // check the source <-> system <==> projects relations
        List<Source> allSources = sourceService.getSourceList(mainCustomerSpace, projectId);
        Assert.assertEquals(CollectionUtils.size(allSources), 2);
        allSystems = cdlProxy.getS3ImportSystemList(mainCustomerSpace);
        Assert.assertEquals(CollectionUtils.size(allSystems), CollectionUtils.size(allSources));
        for (Source s : allSources) {
            ProjectInfo projectInfo = projectService.getProjectBySourceId(mainCustomerSpace, s.getSourceId());
            Assert.assertEquals(projectInfo.getProjectId(), projectId);
        }
    }

}
