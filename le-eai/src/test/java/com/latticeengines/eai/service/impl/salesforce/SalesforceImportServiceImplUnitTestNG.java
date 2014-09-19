package com.latticeengines.eai.service.impl.salesforce;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;

public class SalesforceImportServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void createQuery() {
        Table table = new Table();
        table.setName("Lead");
        
        Attribute firstName = new Attribute();
        firstName.setName("FirstName");
        Attribute lastName = new Attribute();
        lastName.setName("LastName");
        Attribute salutation = new Attribute();
        salutation.setName("Salutation");
        Attribute email = new Attribute();
        email.setName("Email");
        
        table.addAttribute(firstName);
        table.addAttribute(lastName);
        table.addAttribute(salutation);
        table.addAttribute(email);
        
        SalesforceImportServiceImpl svc = new SalesforceImportServiceImpl();
        String query = svc.createQuery(table);
        assertEquals(query, "SELECT FirstName,LastName,Salutation,Email FROM Lead");
    }
}

