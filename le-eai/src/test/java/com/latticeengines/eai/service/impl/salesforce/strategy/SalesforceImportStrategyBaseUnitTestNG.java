package com.latticeengines.eai.service.impl.salesforce.strategy;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class SalesforceImportStrategyBaseUnitTestNG {

    @Test(groups = "unit")
    public void createQuery() {
        Table table = new Table();
        table.setName("Lead");

        Attribute firstName = new Attribute();
        firstName.setPhysicalName("FirstName");
        Attribute lastName = new Attribute();
        lastName.setPhysicalName("LastName");
        Attribute salutation = new Attribute();
        salutation.setPhysicalName("Salutation");
        Attribute email = new Attribute();
        email.setPhysicalName("Email");

        table.addAttribute(firstName);
        table.addAttribute(lastName);
        table.addAttribute(salutation);
        table.addAttribute(email);

        SalesforceImportStrategyBase strategy = new SalesforceImportStrategyBase("Salesforce.AllTables");
        String query = strategy.createQuery(table, null);
        assertEquals(query, "SELECT FirstName,LastName,Salutation,Email FROM Lead");
    }
}
