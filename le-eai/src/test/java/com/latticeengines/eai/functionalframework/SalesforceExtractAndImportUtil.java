package com.latticeengines.eai.functionalframework;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class SalesforceExtractAndImportUtil {

    public static Table createAccountWithNonExistingAttr() {
        Table table = new Table();
        table.setName("Account");

        Attribute id = new Attribute();
        id.setName("SomeNonExistingId");
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
        lastModifiedDate.setName("SomeNonExistingLastModifiedDate");

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
        lk.setName("LK_LastModifiedDate");
        lk.addAttribute(lastModifiedDate.getName());
        table.setLastModifiedKey(lk);
        return table;
    }

}
