package com.latticeengines.propdata.eai.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.eai.context.PropDataContext;
import com.latticeengines.propdata.eai.service.PropDataDBService;
import com.latticeengines.propdata.eai.service.PropDataKey.CommandIdsKey;
import com.latticeengines.propdata.eai.service.PropDataKey.CommandsKey;
import com.latticeengines.propdata.eai.service.PropDataKey.ImportExportKey;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:propdata-eai-context.xml", "classpath:propdata-eai-properties-context.xml" })
public class PropDataDBServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String PROPDATA_OUTPUT = PropDataDBServiceImpl.PROPDATA_OUTPUT;
    private static final String PROPDATA_INPUT = PropDataDBServiceImpl.PROPDATA_INPUT;

    private static final String LEAD_TABLE = "lead";
    private static final String OPPORTUNITY_TABLE = "Opportunity";
    private static final String PAYPAL_TABLE = "PayPal_matching_pipeline";
    private static final String EVENT_TABLE_TABLE = "EventTable";

    private static final String CUSTOMER = "PropData-Pipeline-Tester";

    private static final String LEAD_FILE = ClassLoader.getSystemResource(
            "com/latticeengines/propdata/dataflow/service/impl/Lead.avro").getPath();
    private static final String OPPORTUNITY_FILE = ClassLoader.getSystemResource(
            "com/latticeengines/propdata/dataflow/service/impl/Opportunity.avro").getPath();
    private static final String PAYPALL_FILE = ClassLoader.getSystemResource(
            "com/latticeengines/propdata/dataflow/service/impl/PayPal_matching_pipeline.avro").getPath();
    private static final String EVENT_TABLE_FILE = ClassLoader.getSystemResource(
            "com/latticeengines/propdata/dataflow/service/impl/EventTable.avro").getPath();

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private PropDataDBService propDataDBService;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;
    private PropDataContext previousResponseContext;

    @Test(groups = "functional", dataProvider = "initData", enabled=false)
    public void init(String customer, String table, String inputFile) throws Exception {
        String outputDir = ((PropDataDBServiceImpl) propDataDBService)
                .getDataHdfsPath(customer, table, PROPDATA_OUTPUT);
        HdfsUtils.rmdir(yarnConfiguration, outputDir);

        String inputDir = ((PropDataDBServiceImpl) propDataDBService).getDataHdfsPath(customer, table, PROPDATA_INPUT);

        HdfsUtils.rmdir(yarnConfiguration, inputDir);
        HdfsUtils.mkdir(yarnConfiguration, inputDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputFile, inputDir);

    }

    @Test(groups = "functional", dataProvider = "exportToDBData", dependsOnMethods = { "init" }, enabled=false)
    public void exportToDB(String customer, String table, boolean mapColumn) throws Exception {

        PropDataContext requestContext = getRequestContextForExport(customer, table, mapColumn);
        PropDataContext responseContext = propDataDBService.exportToDB(requestContext);

        Assert.assertNotNull(responseContext);
        Assert.assertNotNull(responseContext.getProperty(ImportExportKey.APPLICATION_ID.getKey(), Integer.class));
        Assert.assertEquals(responseContext.getProperty(ImportExportKey.TABLE.getKey(), String.class), table);
        Assert.assertEquals(responseContext.getProperty(ImportExportKey.CUSTOMER.getKey(), String.class), customer);
    }

    @Test(groups = "functional", dataProvider = "addCommandData", dependsOnMethods = { "exportToDB" }, enabled=false)
    public void addCommand(String customer, String table) {
        PropDataContext requestContext = getRequestContextForAddCommand(customer, table);
        PropDataContext responseContext = propDataDBService.addCommandAndWaitForComplete(requestContext);
        Long commandId = responseContext.getProperty(CommandsKey.COMMAND_ID.getKey(), Long.class);
        System.out.println("CommandId=" + commandId);
        Assert.assertNotNull(commandId);

        previousResponseContext = responseContext;
    }

    @Test(groups = "functional", dataProvider = "importFromDBData", dependsOnMethods = { "addCommand" }, enabled=false)
    public void importFromDB(String customer, String table, String keyCols) throws Exception {

        PropDataContext requestContext = getRequestContextForImport(customer, table, keyCols, previousResponseContext);

        PropDataContext responseContext = propDataDBService.importFromDB(requestContext);

        Assert.assertNotNull(responseContext);
        Assert.assertNotNull(responseContext.getProperty(ImportExportKey.APPLICATION_ID.getKey(), String.class));
        Assert.assertNotNull(responseContext.getProperty(ImportExportKey.TABLE.getKey(), String.class));

    }

    @Test(groups = "functional", dataProvider = "singleImportFromDBData", enabled=false)
    public void createSingleAVROFromTable(String customer, String table, String keyCols) throws Exception {

        String outputDir = ((PropDataDBServiceImpl) propDataDBService)
                .getDataHdfsPath(customer, table, PROPDATA_OUTPUT);
        HdfsUtils.rmdir(yarnConfiguration, outputDir);

        PropDataContext requestContext = getRequestContextForImport(customer, table, keyCols, previousResponseContext);

        PropDataContext responseContext = propDataDBService.createSingleAVROFromTable(requestContext);

        Assert.assertNotNull(responseContext);
        Assert.assertNotNull(responseContext.getProperty(ImportExportKey.APPLICATION_ID.getKey(), String.class));
        Assert.assertNotNull(responseContext.getProperty(ImportExportKey.TABLE.getKey(), String.class));

    }

    @DataProvider(name = "singleImportFromDBData")
    public Object[][] getSingleImportFromDBData() {
        return new Object[][] { { CUSTOMER, PAYPAL_TABLE, "DUNS" } };
    }

    @DataProvider(name = "initData")
    public Object[][] getInitData() {
        return new Object[][] { { CUSTOMER, LEAD_TABLE, LEAD_FILE }, { CUSTOMER, OPPORTUNITY_TABLE, OPPORTUNITY_FILE },
                { CUSTOMER, PAYPAL_TABLE, PAYPALL_FILE }, { CUSTOMER, EVENT_TABLE_TABLE, EVENT_TABLE_FILE }, };
    }

    @DataProvider(name = "exportToDBData")
    public Object[][] getExportToDBData() {
        return new Object[][] { { CUSTOMER, LEAD_TABLE, false }, { CUSTOMER, OPPORTUNITY_TABLE, false },
                { CUSTOMER, PAYPAL_TABLE, false }, { CUSTOMER, EVENT_TABLE_TABLE, true }, };
    }

    @DataProvider(name = "addCommandData")
    public Object[][] getAddCommandData() {
        return new Object[][] { { CUSTOMER, LEAD_TABLE }, { CUSTOMER, OPPORTUNITY_TABLE }, { CUSTOMER, PAYPAL_TABLE },
                { CUSTOMER, EVENT_TABLE_TABLE },

        };
    }

    @DataProvider(name = "importFromDBData")
    public Object[][] getImportFromDBData() {
        return new Object[][] { { CUSTOMER, LEAD_TABLE, "Email" }, { CUSTOMER, OPPORTUNITY_TABLE, "Amount" },
                { CUSTOMER, PAYPAL_TABLE, "DUNS" }, { CUSTOMER, EVENT_TABLE_TABLE, "RowId" }, };
    }

    private PropDataContext getRequestContextForAddCommand(String customer, String table) {
        PropDataContext requestContext = new PropDataContext();

        requestContext.setProperty(CommandIdsKey.CREATED_BY.getKey(), "propdata@lattice-engines.com");

        requestContext.setProperty(CommandsKey.COMMAND_NAME.getKey(), "RunMatchWithLEUniverse");
        requestContext.setProperty(CommandsKey.CONTRACT_EXTERNAL_ID.getKey(), customer);
        requestContext.setProperty(CommandsKey.DEPLOYMENT_EXTERNAL_ID.getKey(), customer);
        requestContext.setProperty(CommandsKey.DESTTABLES.getKey(), "DerivedColumns|Alexa_Source");

        requestContext.setProperty(CommandsKey.IS_DOWNLOADING.getKey(), Boolean.FALSE);

        requestContext.setProperty(CommandsKey.MAX_NUMR_ETRIES.getKey(), 5);
        requestContext.setProperty(CommandsKey.SOURCE_TABLE.getKey(), table);

        return requestContext;
    }

    private PropDataContext getRequestContextForImport(String customer, String table, String keyCols,
            PropDataContext requestContext) {

        if (requestContext == null) {
            requestContext = new PropDataContext();
        }
        requestContext.setProperty(ImportExportKey.CUSTOMER.getKey(), customer);
        requestContext.setProperty(ImportExportKey.TABLE.getKey(), table);
        requestContext.setProperty(ImportExportKey.KEY_COLS.getKey(), keyCols);

        return requestContext;
    }

    private PropDataContext getRequestContextForExport(String customer, String table, boolean mapColumn) {
        PropDataContext requestContext = new PropDataContext();
        requestContext.setProperty(ImportExportKey.CUSTOMER.getKey(), customer);
        requestContext.setProperty(ImportExportKey.TABLE.getKey(), table);
        requestContext.setProperty(ImportExportKey.MAP_COLUMN.getKey(), mapColumn);
        return requestContext;
    }

}
