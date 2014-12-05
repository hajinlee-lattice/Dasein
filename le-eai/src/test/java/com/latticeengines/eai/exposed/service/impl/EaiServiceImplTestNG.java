package com.latticeengines.eai.exposed.service.impl;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;


@Transactional
public class EaiServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    protected static final Log log = LogFactory.getLog(EaiServiceImplTestNG.class);

    private String customer = "Eai-" + System.currentTimeMillis();

    @Test(groups = { "functional", "functional.production" }, enabled = true)
    public void invokeEai() throws Exception {
        SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
        Map<String, String> props = new HashMap<>();

        props.put(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        props.put(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        props.put(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        
        List<Table> tables = new ArrayList<>();
        Table activityType = createMarketoActivityType();
        Table lead = createMarketoLead();
        Table activity = createMarketoActivity();
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);
        
        marketoImportConfig.setSourceType(SourceType.MARKETO);
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
        marketoImportConfig.setProperties(props);
        String targetPath = "/tmp";
        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setCustomer(customer);
        importConfig.addSourceConfiguration(marketoImportConfig);
        importConfig.setTargetPath(targetPath);
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        //FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        //assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }
}