package com.latticeengines.eai.service.impl.marketo;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.service.ImportService;

public class MarketoImportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ImportService marketoImportService;
    
    private List<Table> tables = new ArrayList<>();
    private ImportContext ctx = new ImportContext();
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ctx.setProperty(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        ctx.setProperty(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        ctx.setProperty(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        
        Table activityType = createActivityType();
        Table lead = createLead();
        tables.add(activityType);
        tables.add(lead);
    }
    
    @Test(groups = "functional")
    public void importMetadata() {
        tables = marketoImportService.importMetadata(tables, ctx);
        
        for (Table table : tables) {
            for (Attribute attribute : table.getAttributes()) {
                assertNotNull(attribute.getPhysicalDataType());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "importMetadata" })
    public void importDataAndWriteToHdfs() {
        marketoImportService.importDataAndWriteToHdfs(tables, ctx);
    }

    private Table createActivityType() {
        Table table = new Table();
        table.setName("ActivityType");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("id");
        Attribute name = new Attribute();
        name.setName("name");
        name.setDisplayName("Name");
        name.setLogicalDataType("string");
        table.addAttribute(id);
        table.addAttribute(name);
        return table;
    }
    
    private Table createLead() {
        Table table = new Table();
        table.setName("Lead");

        Attribute id = new Attribute();
        id.setName("id");
        Attribute anonymousIP = new Attribute();
        anonymousIP.setName("anonymousIP");
        Attribute inferredCompany = new Attribute();
        inferredCompany.setName("inferredCompany");
        Attribute inferredCountry = new Attribute();
        inferredCountry.setName("inferredCountry");
        Attribute title = new Attribute();
        title.setName("title");
        Attribute department = new Attribute();
        department.setName("department");
        Attribute unsubscribed = new Attribute();
        unsubscribed.setName("unsubscribed");
        Attribute unsubscribedReason = new Attribute();
        unsubscribedReason.setName("unsubscribedReason");
        Attribute doNotCall = new Attribute();
        doNotCall.setName("doNotCall");
        Attribute country = new Attribute();
        country.setName("country");
        Attribute website = new Attribute();
        website.setName("website");
        Attribute email = new Attribute();
        email.setName("email");
        Attribute leadStatus = new Attribute();
        leadStatus.setName("leadStatus");
        Attribute company = new Attribute();
        company.setName("company");
        Attribute leadSource = new Attribute();
        leadSource.setName("leadSource");
        Attribute industry = new Attribute();
        industry.setName("industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("annualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("numberOfEmployees");
        Attribute doNotCallReason = new Attribute();
        doNotCallReason.setName("doNotCallReason");
        Attribute sicCode = new Attribute();
        sicCode.setName("sicCode");
        Attribute phone = new Attribute();
        phone.setName("phone");
        Attribute facebookReferredEnrollments = new Attribute();
        facebookReferredEnrollments.setName("facebookReferredEnrollments");
        Attribute facebookReferredVisits = new Attribute();
        facebookReferredVisits.setName("facebookReferredVisits");

        table.addAttribute(id);
        table.addAttribute(anonymousIP);
        table.addAttribute(inferredCompany);
        table.addAttribute(inferredCountry);
        table.addAttribute(title);
        table.addAttribute(department);
        table.addAttribute(unsubscribed);
        table.addAttribute(unsubscribedReason);
        table.addAttribute(doNotCall);
        table.addAttribute(country);
        table.addAttribute(website);
        table.addAttribute(email);
        table.addAttribute(leadStatus);
        table.addAttribute(company);
        table.addAttribute(leadSource);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(doNotCallReason);
        table.addAttribute(sicCode);
        table.addAttribute(phone);
        table.addAttribute(facebookReferredEnrollments);
        table.addAttribute(facebookReferredVisits);

        return table;
    }

}
