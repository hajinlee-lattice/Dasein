package com.latticeengines.eai.service.impl.marketo;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.service.ImportService;

public class MarketoImportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ImportService marketoImportService;
    
    private SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
    private ImportContext ctx = new ImportContext();
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ctx.setProperty(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        ctx.setProperty(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        ctx.setProperty(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        
        List<Table> tables = new ArrayList<>();
        Table activityType = createMarketoActivityType();
        Table lead = createMarketoLead();
        Table activity = createMarketoActivity();
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);
        
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
    }
    
    @Test(groups = "functional")
    public void importMetadata() {
        List<Table> tables = marketoImportService.importMetadata(marketoImportConfig, ctx);
        
        for (Table table : tables) {
            for (Attribute attribute : table.getAttributes()) {
                assertNotNull(attribute.getPhysicalDataType());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "importMetadata" })
    public void importDataAndWriteToHdfs() {
        marketoImportService.importDataAndWriteToHdfs(marketoImportConfig, ctx);
    }

}
