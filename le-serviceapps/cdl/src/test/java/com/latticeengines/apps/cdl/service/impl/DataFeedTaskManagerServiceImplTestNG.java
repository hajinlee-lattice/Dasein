package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class DataFeedTaskManagerServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    private DataFeed datafeed = new DataFeed();

    private DataFeedTask task = new DataFeedTask();

    private String taskId = NamingUtils.uuid("DataFeedTask");

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        // setup data feed & data feed task
        datafeed.setName(NamingUtils.timestamp("datafeed"));
        datafeed.setDataCollection(dataCollection);
        datafeed.setStatus(DataFeed.Status.Active);
        datafeed.setTenant(MultiTenantContext.getTenant());

        task.setDataFeed(datafeed);
        task.setImportTemplate(getTemplateTable());
        task.setActiveJob("Not specified");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("File");
        task.setStatus(DataFeedTask.Status.Active);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setLastUpdated(new Date());
        task.setUniqueId(taskId);
        task.setSourceConfig("Not specified");
        datafeed.addTask(task);

        datafeedEntityMgr.create(datafeed);
    }

    private Table getTemplateTable() {
        Table templateTable = SchemaRepository.instance().getSchema(BusinessEntity.Account, true, false, true);
        Attribute userAttr1 = new Attribute();
        userAttr1.setPhysicalDataType("String");
        userAttr1.setFundamentalType(FundamentalType.ALPHA);
        userAttr1.setName("user_Att1");
        userAttr1.setDisplayName("Attr1");
        templateTable.addAttribute(userAttr1);
        Attribute userAttr2 = new Attribute();
        userAttr2.setPhysicalDataType("Int");
        userAttr2.setFundamentalType(FundamentalType.ALPHA);
        userAttr2.setName("user_Att2");
        userAttr2.setDisplayName("Attr2");
        templateTable.addAttribute(userAttr2);
        for (Attribute attr : templateTable.getAttributes()) {
            if (attr.getName().equalsIgnoreCase("AnnualRevenue")) {
                attr.setPhysicalDataType("Long");
            }
            if (attr.getName().equalsIgnoreCase("NumberOfEmployees")) {
                attr.setFundamentalType(FundamentalType.ALPHA);
            }
        }
        return templateTable;
    }

    @Test(groups = "functional")
    public void testDiagnostic() {
        ImportTemplateDiagnostic diagnostic = dataFeedTaskManagerService.diagnostic(mainCustomerSpace, taskId);
        Assert.assertNotNull(diagnostic);
        Assert.assertEquals(diagnostic.getErrors().size(), 1);
        Assert.assertEquals(diagnostic.getWarnings().size(), 2);
        Assert.assertTrue(diagnostic.getErrors().get(0).contains("AnnualRevenue"));
        Assert.assertTrue(diagnostic.getWarnings().get(0).contains("user_Att2")
                || diagnostic.getWarnings().get(1).contains("user_Att2") );
        Assert.assertTrue(diagnostic.getWarnings().get(0).contains("NumberOfEmployees")
                || diagnostic.getWarnings().get(1).contains("NumberOfEmployees") );
    }
}
