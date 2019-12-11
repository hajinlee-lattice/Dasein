package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.EntityType;

public class CDLExternalSystemUnitTestNG {

    @Test
    public void testSimpleTemplate() {
        List<SimpleTemplateMetadata> templateMetadata = new ArrayList<>();
        SimpleTemplateMetadata webVisit = new SimpleTemplateMetadata();
        webVisit.setEntityType(EntityType.WebVisit);
        SimpleTemplateMetadata.SimpleTemplateAttribute visitDate = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        visitDate.setDisplayName("visit_date");
        visitDate.setName(InterfaceName.WebVisitDate.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute pageurl = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        pageurl.setDisplayName("page_url");
        pageurl.setName(InterfaceName.WebVisitPageUrl.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute userId = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        userId.setDisplayName("user_id");
        userId.setName(InterfaceName.UserId.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute company_name = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        company_name.setDisplayName("company_name");
        company_name.setName(InterfaceName.CompanyName.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute company_city = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        company_city.setDisplayName("company_city");
        company_city.setName(InterfaceName.City.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute company_state = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        company_state.setDisplayName("company_state");
        company_state.setName(InterfaceName.State.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute company_country = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        company_country.setDisplayName("company_country");
        company_country.setName(InterfaceName.Country.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute duns = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        duns.setDisplayName("duns");
        duns.setName(InterfaceName.DUNS.name());
        SimpleTemplateMetadata.SimpleTemplateAttribute source_medium = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        source_medium.setDisplayName("source_medium");
        source_medium.setName(InterfaceName.SourceMedium.name());
        List<SimpleTemplateMetadata.SimpleTemplateAttribute> visitAttributes =
                new ArrayList<>(Arrays.asList(visitDate, pageurl, userId, company_name, company_city, company_state, company_country, duns, source_medium));
        webVisit.setStandardAttributes(visitAttributes);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom1 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom1.setDisplayName("visit_id");
        custom1.setName("user_visit_id");
        custom1.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom2 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom2.setDisplayName("company_website");
        custom2.setName("user_company_website");
        custom2.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom3 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom3.setDisplayName("company_website");
        custom3.setName("user_company_website");
        custom3.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom4 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom4.setDisplayName("company_zip");
        custom4.setName("user_company_zip");
        custom4.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom5 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom5.setDisplayName("custparam1");
        custom5.setName("user_custparam1");
        custom5.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom6 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom6.setDisplayName("custparam2");
        custom6.setName("user_custparam2");
        custom6.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom7 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom7.setDisplayName("custparam3");
        custom7.setName("user_custparam3");
        custom7.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom8 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom8.setDisplayName("custparam4");
        custom8.setName("user_custparam4");
        custom8.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom9 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom9.setDisplayName("lattice_id");
        custom9.setName("user_lattice_id");
        custom9.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom10 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom10.setDisplayName("contact_id");
        custom10.setName("user_contact_id");
        custom10.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom11 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom11.setDisplayName("campaign_id");
        custom11.setName("user_campaign_id");
        custom11.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom12 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom12.setDisplayName("map_cookie_id");
        custom12.setName("user_map_cookie_id");
        custom12.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom13 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom13.setDisplayName("dmp_cookie_id");
        custom13.setName("user_dmp_cookie_id");
        custom13.setPhysicalDataType(Schema.Type.STRING);

        SimpleTemplateMetadata.SimpleTemplateAttribute custom14 = new SimpleTemplateMetadata.SimpleTemplateAttribute();
        custom14.setDisplayName("Pattern Name");
        custom14.setName("user_pattern_name");
        custom14.setPhysicalDataType(Schema.Type.STRING);

        List<SimpleTemplateMetadata.SimpleTemplateAttribute> visitCustomAttributes =
                new ArrayList<>(Arrays.asList(custom1, custom2, custom3, custom4, custom5, custom6, custom7, custom8,
                        custom9, custom10, custom11, custom12, custom13, custom14));
        webVisit.setCustomerAttributes(visitCustomAttributes);



        SimpleTemplateMetadata webVisitPath = new SimpleTemplateMetadata();
        webVisitPath.setEntityType(EntityType.WebVisitPathPattern);
        SimpleTemplateMetadata sourceMedium = new SimpleTemplateMetadata();
        sourceMedium.setEntityType(EntityType.WebVisitSourceMedium);

        templateMetadata.add(webVisit);
        templateMetadata.add(webVisitPath);
        templateMetadata.add(sourceMedium);

        System.out.println(JsonUtils.serialize(templateMetadata));


    }

    @Test(groups = "unit")
    public void testExternalSystem() {
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("accountId");
        crmIds.add("testId");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);

        List<Pair<String, String>> idMappings = new ArrayList<>();
        idMappings.add(Pair.of("accountId", "AccountDisplayName"));
        idMappings.add(Pair.of("testId", "TestDisplayName"));
        cdlExternalSystem.setIdMapping(idMappings);

        cdlExternalSystem.setPid(1L);
        List<Pair<String, String>> outIdMappings = cdlExternalSystem.getIdMappingList();
        Assert.assertTrue(outIdMappings.size() == 2);
        Assert.assertTrue(outIdMappings.get(0).getLeft().equals("accountId"));

        String cdlExternalSystemStr = JsonUtils.serialize(cdlExternalSystem);
        Assert.assertNotNull(cdlExternalSystemStr);
        Assert.assertTrue(cdlExternalSystemStr.contains(InterfaceName.SalesforceSandboxAccountID.name()));


        Assert.assertTrue(cdlExternalSystem.getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
    }
}
