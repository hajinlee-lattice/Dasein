package com.latticeengines.pls.functionalframework;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class SalesforceExtractAndImportUtil {

    public static Table createLead() {
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

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        pk.setDisplayName(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LastModifiedDate");
        lk.setDisplayName(lastModifiedDate.getName());
        lk.addAttribute(lastModifiedDate.getName());
        lk.setLastModifiedTimestamp(1000000000000L);
        table.setLastModifiedKey(lk);
        return table;
    }

    public static Table createAccount() {
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
        street.setName("BillingStreet");
        Attribute city = new Attribute();
        city.setName("BillingCity");
        Attribute state = new Attribute();
        state.setName("BillingState");
        Attribute postalCode = new Attribute();
        postalCode.setName("BillingPostalCode");
        Attribute country = new Attribute();
        country.setName("BillingCountry");
        Attribute website = new Attribute();
        website.setName("Website");
        Attribute sic = new Attribute();
        sic.setName("Sic");
        Attribute industry = new Attribute();
        industry.setName("Industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("AnnualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("NumberOfEmployees");
        Attribute lastActivityDate = new Attribute();
        lastActivityDate.setName("LastActivityDate");
        Attribute lastViewedDate = new Attribute();
        lastViewedDate.setName("LastViewedDate");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");
        Attribute ownerId = new Attribute();
        ownerId.setName("OwnerId");
        Attribute ownership = new Attribute();
        ownership.setName("Ownership");
        Attribute rating = new Attribute();
        rating.setName("Rating");
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");

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
        table.addAttribute(sic);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(lastActivityDate);
        table.addAttribute(createdDate);
        table.addAttribute(ownerId);
        table.addAttribute(rating);
        table.addAttribute(lastModifiedDate);

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_CreatedDate");
        lk.addAttribute(createdDate.getName());
        table.setLastModifiedKey(lk);
        return table;
    }
    
    public static Table createAccountWithNonExistingAttr(){
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
        street.setName("BillingStreet");
        Attribute city = new Attribute();
        city.setName("BillingCity");
        Attribute state = new Attribute();
        state.setName("ShippingState");
        Attribute postalCode = new Attribute();
        postalCode.setName("BillingPostalCode");
        Attribute country = new Attribute();
        country.setName("BillingCountry");
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
        Attribute lastModifiedDate = new Attribute();
        lastModifiedDate.setName("LastModifiedDate");

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
        table.addAttribute(lastModifiedDate);

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_CreatedDate");
        lk.addAttribute(createdDate.getName());
        table.setLastModifiedKey(lk);
        return table;
    }

    public static Table createOpportunity() {
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

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LastModifiedDate");
        lk.addAttribute(lastModifiedDate.getName());
        table.setLastModifiedKey(lk);
        return table;
    }

    public static Table createContact() {
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

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LastModifiedDate");
        lk.addAttribute(lastModifiedDate.getName());
        table.setLastModifiedKey(lk);

        return table;
    }

    public static Table createOpportunityContactRole() {
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

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LastModifiedDate");
        lk.addAttribute(lastModifiedDate.getName());
        table.setLastModifiedKey(lk);

        return table;
    }

}
