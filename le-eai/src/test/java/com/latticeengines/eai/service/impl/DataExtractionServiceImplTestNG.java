package com.latticeengines.eai.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.AppmasterServiceClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.appmaster.service.AppmasterServiceRequest;
import com.latticeengines.eai.appmaster.service.AppMasterServiceResponse;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class DataExtractionServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    private String customer = "SFDC-Eai-Customer";

    @Autowired
    private AppmasterServiceClient appmasterServiceClient;

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customer, customer, "defaultspaceId", spaceInfo);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010oIogZVEFGbL3n0qiAp6F66TC");
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath("Production", customer).toString());
        crmCredentialZKService.removeCredentials(customer, customer, true);
    }

    @Test(groups = "functional")
    public void extractAndImport() throws Exception {
        List<Table> tables = new ArrayList<>();
        Table lead = createLead();
        Table account = createAccount();
        Table opportunity = createOpportunity();
        Table contact = createContact();
        Table contactRole = createOpportunityContactRole();
        tables.add(lead);
        tables.add(account);
        tables.add(opportunity);
        tables.add(contact);
        tables.add(contactRole);

        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);
        salesforceConfig.setTables(tables);

        importConfig.addSourceConfiguration(salesforceConfig);
        importConfig.setCustomer(customer);
        dataExtractionService.submitExtractAndImportJob(importConfig);
        
        BaseObject request = new AppmasterServiceRequest();
        Thread.sleep(20000);
        BaseResponseObject obj = ((MindAppmasterServiceClient) appmasterServiceClient).doMindRequest(request);
        System.out.println(((AppMasterServiceResponse)obj));

        Thread.sleep(30000L);
    }

    private Table createLead() {
        Table table = new Table();
        table.setName("Lead");

        Attribute id = new Attribute();
        id.setName("Id");
        Attribute firstName = new Attribute();
        firstName.setName("FirstName");
        Attribute lastName = new Attribute();
        lastName.setName("LastName");
        Attribute salutation = new Attribute();
        salutation.setName("Salutation");
        Attribute title = new Attribute();
        title.setName("Title");
        Attribute street = new Attribute();
        street.setName("Street");
        Attribute city = new Attribute();
        city.setName("City");
        Attribute state = new Attribute();
        state.setName("State");
        Attribute postalCode = new Attribute();
        postalCode.setName("PostalCode");
        Attribute country = new Attribute();
        country.setName("Country");
        Attribute website = new Attribute();
        website.setName("Website");
        Attribute email = new Attribute();
        email.setName("Email");
        Attribute status = new Attribute();
        status.setName("Status");
        Attribute company = new Attribute();
        company.setName("Company");
        Attribute leadSource = new Attribute();
        leadSource.setName("LeadSource");
        Attribute industry = new Attribute();
        industry.setName("Industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("AnnualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("NumberOfEmployees");
        Attribute converted = new Attribute();
        converted.setName("IsConverted");
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");
        Attribute convertedOpportunityId = new Attribute();
        convertedOpportunityId.setName("ConvertedOpportunityId");
        Attribute ownerId = new Attribute();
        ownerId.setName("OwnerId");

        table.addAttribute(id);
        table.addAttribute(firstName);
        table.addAttribute(lastName);
        table.addAttribute(salutation);
        table.addAttribute(title);
        table.addAttribute(street);
        table.addAttribute(city);
        table.addAttribute(state);
        table.addAttribute(postalCode);
        table.addAttribute(country);
        table.addAttribute(website);
        table.addAttribute(email);
        table.addAttribute(status);
        table.addAttribute(company);
        table.addAttribute(leadSource);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(converted);
        table.addAttribute(lastModifiedDate);
        table.addAttribute(createdDate);
        table.addAttribute(convertedOpportunityId);
        table.addAttribute(ownerId);

        return table;
    }

    private Table createAccount() {
        Table table = new Table();
        table.setName("Account");

        Attribute id = new Attribute();
        id.setName("Id");
        Attribute accountSource = new Attribute();
        accountSource.setName("AccountSource");
        Attribute name = new Attribute();
        name.setName("Name");
        Attribute tickerSymbol = new Attribute();
        tickerSymbol.setName("TickerSymbol");
        Attribute type = new Attribute();
        type.setName("Type");
        Attribute street = new Attribute();
        street.setName("Street");
        Attribute city = new Attribute();
        city.setName("City");
        Attribute state = new Attribute();
        state.setName("State");
        Attribute postalCode = new Attribute();
        postalCode.setName("PostalCode");
        Attribute country = new Attribute();
        country.setName("Country");
        Attribute website = new Attribute();
        website.setName("Website");
        Attribute naicsCode = new Attribute();
        naicsCode.setName("NaicsCode");
        Attribute status = new Attribute();
        status.setName("Status");
        Attribute company = new Attribute();
        company.setName("Company");
        Attribute sic = new Attribute();
        sic.setName("Sic");
        Attribute industry = new Attribute();
        industry.setName("Industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("AnnualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("NumberOfEmployees");
        Attribute converted = new Attribute();
        converted.setName("IsConverted");
        Attribute lastActivityDate = new Attribute();
        lastActivityDate.setName("LastActivityDate");
        Attribute lastViewedDate = new Attribute();
        lastViewedDate.setName("LastViewedDate");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");
        Attribute ownerId = new Attribute();
        ownerId.setName("OwnerId");
        Attribute salutation = new Attribute();
        salutation.setName("Salutation");
        Attribute ownership = new Attribute();
        ownership.setName("Ownership");
        Attribute rating = new Attribute();
        rating.setName("Rating");

        table.addAttribute(id);
        table.addAttribute(accountSource);
        table.addAttribute(name);
        table.addAttribute(tickerSymbol);
        table.addAttribute(type);
        table.addAttribute(street);
        table.addAttribute(city);
        table.addAttribute(state);
        table.addAttribute(postalCode);
        table.addAttribute(country);
        table.addAttribute(website);
        table.addAttribute(naicsCode);
        table.addAttribute(status);
        table.addAttribute(company);
        table.addAttribute(sic);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(converted);
        table.addAttribute(lastActivityDate);
        table.addAttribute(createdDate);
        table.addAttribute(ownerId);
        table.addAttribute(salutation);
        table.addAttribute(rating);

        return table;
    }

    private Table createOpportunity() {
        Table table = new Table();
        table.setName("Opportunity");

        Attribute id = new Attribute();
        id.setName("Id");
        Attribute accountId = new Attribute();
        accountId.setName("AccountId");
        Attribute won = new Attribute();
        won.setName("IsWon");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");
        Attribute stageName = new Attribute();
        stageName.setName("StageName");
        Attribute amount = new Attribute();
        amount.setName("Amount");
        Attribute leadSource = new Attribute();
        leadSource.setName("LeadSource");
        Attribute closed = new Attribute();
        closed.setName("IsClosed");
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");

        table.addAttribute(id);
        table.addAttribute(accountId);
        table.addAttribute(won);
        table.addAttribute(stageName);
        table.addAttribute(amount);
        table.addAttribute(closed);
        table.addAttribute(leadSource);
        table.addAttribute(lastModifiedDate);
        table.addAttribute(createdDate);

        return table;
    }

    private Table createContact() {
        Table table = new Table();
        table.setName("Contact");

        Attribute id = new Attribute();
        id.setName("Id");
        Attribute accountId = new Attribute();
        accountId.setName("AccountId");
        Attribute email = new Attribute();
        email.setName("Email");
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");

        table.addAttribute(id);
        table.addAttribute(accountId);
        table.addAttribute(email);
        table.addAttribute(lastModifiedDate);

        return table;
    }

    private Table createOpportunityContactRole() {
        Table table = new Table();
        table.setName("OpportunityContactRole");

        Attribute id = new Attribute();
        id.setName("Id");
        Attribute primary = new Attribute();
        primary.setName("IsPrimary");
        Attribute role = new Attribute();
        role.setName("Role");
        Attribute contactId = new Attribute();
        contactId.setName("ContactId");
        Attribute opportunityId = new Attribute();
        opportunityId.setName("OpportunityId");
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");

        table.addAttribute(id);
        table.addAttribute(primary);
        table.addAttribute(role);
        table.addAttribute(contactId);
        table.addAttribute(opportunityId);
        table.addAttribute(lastModifiedDate);
        table.addAttribute(createdDate);

        return table;
    }
}
