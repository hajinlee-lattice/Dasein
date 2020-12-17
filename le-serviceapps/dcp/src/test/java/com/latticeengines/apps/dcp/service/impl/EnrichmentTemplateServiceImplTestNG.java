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
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.security.exposed.service.TenantService;

public class EnrichmentTemplateServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @Inject
    private TenantService tenantService;

    private static final String sourceIdTemplate = "Source_%s";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        mainTestTenant.setSubscriberNumber(SUBSRIBER_NUMBER_MANY_DOMAINS);
        tenantService.updateTenant(mainTestTenant);
    }

    private String getRandomSourceId () {
        return String.format(sourceIdTemplate, RandomStringUtils.randomAlphanumeric(6));
    }

    @Test(groups = "functional")
    public void testCreateTemplateFromLayout() {
        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String userEmail = "user@dnb.com";
        String tenantId = "PropDataService.PropDataService.Production";
        String rndSrcId = getRandomSourceId();
        List<String> elementList = Arrays.asList("primaryname", "duns_number", //
                "dnbassessment_decisionheadqtr_duns","dnbassessment_decisionheadqtr_decisionpowerscore");

        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);
        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);
        layout.setSourceId(rndSrcId);
        layout.setElements(elementList);

        ResponseDocument<String> createLayoutResult = enrichmentLayoutService.create(tenantId, layout);

        Assert.assertNotNull(createLayoutResult);

        String templateName = "Test_Enrichment_Template";

        ResponseDocument<String> createTemplateResult = enrichmentTemplateService.create(layoutId, templateName);

        Assert.assertNotNull(createTemplateResult);
        Assert.assertTrue(createTemplateResult.isSuccess(), "Enrichment Template is not valid");
    }
}
