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

public class EnrichmentTemplateServiceImplTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @Inject
    private TenantService tenantService;

    private static final String sourceIdTemplate = "Source_%s";

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

        ResponseDocument<String> createTemplateResult = enrichmentTemplateService.create(tenantId, layoutId, templateName);

        Assert.assertNotNull(createTemplateResult);
        Assert.assertTrue(createTemplateResult.isSuccess(), "Enrichment Template is not valid");
    }

    @Test(groups = "deployment-dcp")
    public void testCreateTemplate() {
        EnrichmentTemplate template = new EnrichmentTemplate();
        template.setDomain(DataDomain.SalesMarketing);
        template.setRecordType(DataRecordType.Domain);

        List<String> elements = Arrays.asList("primaryname", "duns_number");
        template.setElements(elements);
        template.setCreatedBy("testUser@dnb.com");

        ResponseDocument<String> createTemplateResult = enrichmentTemplateService.create(template);

        Assert.assertNotNull(createTemplateResult);
        Assert.assertTrue(createTemplateResult.isSuccess(), "Enrichment Template is not valid");
    }

    @Test(groups = "deployment-dcp")
    public void testListTemplates() {
        EnrichmentLayout layout1 = createLayout(DataDomain.Finance, DataRecordType.Analytical);
        EnrichmentLayout layout2 = createLayout(DataDomain.SalesMarketing, DataRecordType.Domain);
        String tenantId = "PropDataService.PropDataService.Production";

        ResponseDocument<String> createTemplateResult1 = enrichmentTemplateService.create(tenantId, layout1.getLayoutId(),
                "template1");

        Assert.assertNotNull(createTemplateResult1);
        Assert.assertTrue(createTemplateResult1.isSuccess(), "Enrichment Template is not valid");

        ResponseDocument<String> createTemplateResult2 = enrichmentTemplateService.create(tenantId, layout2.getLayoutId(),
                "template2");

        Assert.assertNotNull(createTemplateResult2);
        Assert.assertTrue(createTemplateResult2.isSuccess(), "Enrichment Template is not valid");

        List<EnrichmentTemplateSummary> summaries1 = enrichmentTemplateService
                .getEnrichmentTemplates(new ListEnrichmentTemplateRequest(mainTestTenant.getId(),
                        DataDomain.Finance.getDisplayName(), DataRecordType.Analytical.getDisplayName(), false, "ALL"));
        Assert.assertEquals(summaries1.size(), 1);

        List<EnrichmentTemplateSummary> summaries2 = enrichmentTemplateService.getEnrichmentTemplates(
                new ListEnrichmentTemplateRequest(mainTestTenant.getId(), "ALL", "ALL", false, "testUser@dnb.com"));
        Assert.assertEquals(summaries2.size(), 2);

        List<EnrichmentTemplateSummary> summaries3 = enrichmentTemplateService
                .getEnrichmentTemplates(new ListEnrichmentTemplateRequest(mainTestTenant.getId(),
                        DataDomain.Compliance.getDisplayName(), "ALL", false, "ALL"));
        Assert.assertEquals(summaries3.size(), 0);
    }
}
