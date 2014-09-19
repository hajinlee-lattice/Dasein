package com.latticeengines.eai.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

public class DataExtractionServiceImplTestNG extends EaiFunctionalTestNGBase {
    
    @Autowired
    private DataExtractionServiceImpl dataExtractionService;

    @Test(groups = "functional")
    public void importData() throws Exception {
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
        
        List<Table> tables = new ArrayList<>();
        tables.add(table);
        dataExtractionService.extractAndImport(tables);
        Thread.sleep(20000L);
    }
}
