package com.latticeengines.eai.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.ImportProperty;

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
        Attribute convertedDate = new Attribute();
        convertedDate.setName("ConvertedDate");
        Attribute createdDate = new Attribute();
        createdDate.setName("CreatedDate");

        table.addAttribute(firstName);
        table.addAttribute(lastName);
        table.addAttribute(salutation);
        table.addAttribute(email);
        table.addAttribute(status);
        table.addAttribute(company);
        table.addAttribute(leadSource);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(converted);
        table.addAttribute(convertedDate);
        table.addAttribute(createdDate);

        List<Table> tables = new ArrayList<>();
        tables.add(table);
        Configuration config = new YarnConfiguration();
        ImportContext context = new ImportContext();
        context.setProperty(ImportProperty.HADOOPCONFIG, config);
        context.setProperty(ImportProperty.TARGETPATH, "/tmp");
        dataExtractionService.extractAndImport(tables, context);
        Thread.sleep(20000L);
    }
}
