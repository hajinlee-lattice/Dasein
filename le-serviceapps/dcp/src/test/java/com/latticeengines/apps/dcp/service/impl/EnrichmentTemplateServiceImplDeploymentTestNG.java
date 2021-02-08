package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;
import com.latticeengines.security.exposed.service.TenantService;

public class EnrichmentTemplateServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @Inject
    private TenantService tenantService;

    private static final String sourceIdTemplate = "Source_%s";

    private EnrichmentTemplateSummary summary;

    @BeforeClass(groups = "deployment-dcp")
    public void setup() {
        setupTestEnvironment();
        mainTestTenant.setSubscriberNumber(SUBSRIBER_NUMBER_MANY_DOMAINS);
        tenantService.updateTenant(mainTestTenant);
    }

    private String getRandomSourceId() {
        return String.format(sourceIdTemplate, RandomStringUtils.randomAlphanumeric(6));
    }

    private EnrichmentLayout createLayout(DataDomain domain, DataRecordType recordType) {
        EnrichmentLayout layout = new EnrichmentLayout();

        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String userEmail = "user@dnb.com";
        String sourceId = getRandomSourceId();

        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);
        layout.setDomain(domain);
        layout.setRecordType(recordType);
        layout.setSourceId(sourceId);

        return layout;
    }

    @Test(groups = "deployment-dcp")
    public void testCreateTemplateFromLayout() {
        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String tenantId = "PropDataService.PropDataService.Production";

        EnrichmentLayout layout = createLayout(DataDomain.SalesMarketing, DataRecordType.Domain);

        ResponseDocument<String> createLayoutResult = enrichmentLayoutService.create(tenantId, layout);

        Assert.assertNotNull(createLayoutResult);

        String templateName = "Test_Enrichment_Template";

        EnrichmentTemplateSummary createTemplateResult = enrichmentTemplateService.create(tenantId, layoutId, templateName);

        Assert.assertNotNull(createTemplateResult, "Template creation from layout failed");
    }

    @Test(groups = "deployment-dcp")
    public void testCreateTemplate() {
        EnrichmentTemplate template = new EnrichmentTemplate();
        template.setDomain(DataDomain.SalesMarketing);
        template.setRecordType(DataRecordType.Domain);

        List<String> elements = Arrays.asList("primaryname", "duns_number");
        template.setElements(elements);
        template.setCreatedBy("testUser@dnb.com");

        summary = enrichmentTemplateService.create(template);

        Assert.assertNotNull(summary, "Template creation failed");
    }

    @Test(groups = "deployment-dcp")
    public void testListTemplates() {
        EnrichmentLayout layout1 = createLayout(DataDomain.Finance, DataRecordType.Analytical);
        EnrichmentLayout layout2 = createLayout(DataDomain.SalesMarketing, DataRecordType.Domain);
        String tenantId = "PropDataService.PropDataService.Production";

        EnrichmentTemplateSummary createTemplateResult1 = enrichmentTemplateService.create(tenantId, layout1.getLayoutId(),
                "template1");

        Assert.assertNotNull(createTemplateResult1);

        EnrichmentTemplateSummary createTemplateResult2 = enrichmentTemplateService.create(tenantId, layout2.getLayoutId(),
                "template2");

        Assert.assertNotNull(createTemplateResult2);

        List<EnrichmentTemplateSummary> summaries1 = enrichmentTemplateService
                .listEnrichmentTemplates(new ListEnrichmentTemplateRequest(mainTestTenant.getId(),
                        DataDomain.Finance.getDisplayName(), DataRecordType.Analytical.getDisplayName(), false, "ALL"));
        Assert.assertEquals(summaries1.size(), 1);

        List<EnrichmentTemplateSummary> summaries2 = enrichmentTemplateService.listEnrichmentTemplates(
                new ListEnrichmentTemplateRequest(mainTestTenant.getId(), "ALL", "ALL", false, "testUser@dnb.com"));
        Assert.assertEquals(summaries2.size(), 2);

        List<EnrichmentTemplateSummary> summaries3 = enrichmentTemplateService
                .listEnrichmentTemplates(new ListEnrichmentTemplateRequest(mainTestTenant.getId(),
                        DataDomain.Compliance.getDisplayName(), "ALL", false, "ALL"));
        Assert.assertEquals(summaries3.size(), 0);
    }

    @Test(groups = "deployment-dcp", dependsOnMethods = "testCreateTemplate")
    public void testGetTemplate() {
        EnrichmentTemplateSummary result = enrichmentTemplateService.getEnrichmentTemplate(summary.templateId);

        Assert.assertNotNull(result, "Could not find the stored template");
        Assert.assertEquals(result.elements, summary.elements, "Did not find correct template");
        Assert.assertEquals(result.templateId, summary.templateId, "Did not find correct template");
        Assert.assertEquals(result.templateName, summary.templateName, "Did not find correct template");
        Assert.assertEquals(result.createdBy, summary.createdBy, "Did not find correct template");
        Assert.assertEquals(result.domain, summary.domain, "Did not find correct template");
        Assert.assertEquals(result.recordType, summary.recordType, "Did not find correct template");
    }
}
