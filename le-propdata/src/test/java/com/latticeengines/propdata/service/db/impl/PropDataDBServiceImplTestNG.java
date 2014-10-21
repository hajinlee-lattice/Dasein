package com.latticeengines.propdata.service.db.impl;

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
import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.propdata.service.db.PropDataDBService;
import com.latticeengines.propdata.service.db.PropDataKey;
import com.latticeengines.propdata.service.db.impl.PropDataDBServiceImpl;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:propdata-db-context.xml",
		"classpath:propdata-properties-context.xml" })
public class PropDataDBServiceImplTestNG extends
		AbstractTestNGSpringContextTests {

	private static final String PROPDATA_OUTPUT = PropDataDBServiceImpl.PROPDATA_OUTPUT;
	private static final String PROPDATA_INPUT = PropDataDBServiceImpl.PROPDATA_INPUT;

	private static final String LEAD_TABLE = "lead";
	private static final String OPPORTUNITY_TABLE = "Opportunity";
	private static final String PAYPAL_TABLE = "PayPal_matching_pipeline";

	private static final String CUSTOMER = "PropData-Pipeline-Tester";

	private static final String LEAD_FILE = ClassLoader.getSystemResource(
			"com/latticeengines/propdata/dataflow/service/impl/Lead.avro")
			.getPath();
	private static final String OPPORTUNITY_FILE = ClassLoader
			.getSystemResource(
					"com/latticeengines/propdata/dataflow/service/impl/Opportunity.avro")
			.getPath();
	private static final String PAYPALL_FILE = ClassLoader
			.getSystemResource(
					"com/latticeengines/propdata/dataflow/service/impl/PayPal_matching_pipeline.avro")
			.getPath();

	@Autowired
	protected Configuration yarnConfiguration;

	@Autowired
	private PropDataDBService propDataDBService;

	@Value("${dataplatform.customer.basedir}")
	protected String customerBaseDir;
	private PropDataContext previousResponseContext;

	@Test(groups = "functional", dataProvider = "initData")
	public void init(String customer, String table, String inputFile)
			throws Exception {
		String outputDir = ((PropDataDBServiceImpl) propDataDBService)
				.getDataHdfsPath(customer, table, PROPDATA_OUTPUT);
		HdfsUtils.rmdir(yarnConfiguration, outputDir);

		String inputDir = ((PropDataDBServiceImpl) propDataDBService)
				.getDataHdfsPath(customer, table, PROPDATA_INPUT);

		HdfsUtils.rmdir(yarnConfiguration, inputDir);
		HdfsUtils.mkdir(yarnConfiguration, inputDir);
		HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputFile, inputDir);

	}

	@Test(groups = "functional", dataProvider = "exportToDBData", dependsOnMethods = { "init" })
	public void exportToDB(String customer, String table) throws Exception {

		PropDataContext requestContext = getRequestContextForExport(customer,
				table);
		PropDataContext responseContext = propDataDBService
				.exportToDB(requestContext);

		Assert.assertNotNull(responseContext);
		Assert.assertNotNull(responseContext.getProperty(
				PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
				Integer.class));
		Assert.assertEquals(responseContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class),
				table);
		Assert.assertEquals(responseContext.getProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), String.class),
				customer);
	}

	@Test(groups = "functional", dataProvider = "addCommandData", dependsOnMethods = { "exportToDB" })
	public void addCommand(String customer, String table) {
		PropDataContext requestContext = getRequestContextForAddCommand(
				customer, table);
		PropDataContext responseContext = propDataDBService
				.addCommandAndWaitForComplete(requestContext);
		Long commandId = responseContext.getProperty(
				PropDataKey.CommandsKey.COMMAND_ID.getValue(), Long.class);
		System.out.println("CommandId=" + commandId);
		Assert.assertNotNull(commandId);

		previousResponseContext = responseContext;
	}

	@Test(groups = "functional", dataProvider = "importFromDBData", dependsOnMethods = { "addCommand" })
	public void importFromDB(String customer, String table, String keyCols)
			throws Exception {

		PropDataContext requestContext = getRequestContextForImport(customer,
				table, keyCols, previousResponseContext);

		PropDataContext responseContext = propDataDBService
				.importFromDB(requestContext);

		Assert.assertNotNull(responseContext);
		Assert.assertNotNull(responseContext.getProperty(
				PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
				String.class));
		Assert.assertNotNull(responseContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class));

	}

	@Test(groups = "functional", dataProvider = "singleImportFromDBData")
	public void createSingleAVROFromTable(String customer, String table,
			String keyCols) throws Exception {

		String outputDir = ((PropDataDBServiceImpl) propDataDBService)
				.getDataHdfsPath(customer, table, PROPDATA_OUTPUT);
		HdfsUtils.rmdir(yarnConfiguration, outputDir);

		PropDataContext requestContext = getRequestContextForImport(customer,
				table, keyCols, previousResponseContext);

		PropDataContext responseContext = propDataDBService
				.createSingleAVROFromTable(requestContext);

		Assert.assertNotNull(responseContext);
		Assert.assertNotNull(responseContext.getProperty(
				PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
				String.class));
		Assert.assertNotNull(responseContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class));

	}

	@DataProvider(name = "singleImportFromDBData")
	public Object[][] getSingleImportFromDBData() {
		return new Object[][] { { CUSTOMER, PAYPAL_TABLE, "DUNS" } };
	}

	@DataProvider(name = "initData")
	public Object[][] getInitData() {
		return new Object[][] { { CUSTOMER, LEAD_TABLE, LEAD_FILE },
				{ CUSTOMER, OPPORTUNITY_TABLE, OPPORTUNITY_FILE },
				{ CUSTOMER, PAYPAL_TABLE, PAYPALL_FILE }, };
	}

	@DataProvider(name = "exportToDBData")
	public Object[][] getExportToDBData() {
		return new Object[][] { { CUSTOMER, LEAD_TABLE },
				{ CUSTOMER, OPPORTUNITY_TABLE }, { CUSTOMER, PAYPAL_TABLE }, };
	}

	@DataProvider(name = "addCommandData")
	public Object[][] getAddCommandData() {
		return new Object[][] { { CUSTOMER, LEAD_TABLE },
				{ CUSTOMER, OPPORTUNITY_TABLE }, { CUSTOMER, PAYPAL_TABLE },

		};
	}

	@DataProvider(name = "importFromDBData")
	public Object[][] getImportFromDBData() {
		return new Object[][] { { CUSTOMER, LEAD_TABLE, "Email" },
				{ CUSTOMER, OPPORTUNITY_TABLE, "Amount" },
				{ CUSTOMER, PAYPAL_TABLE, "DUNS" }, };
	}

	private PropDataContext getRequestContextForAddCommand(String customer,
			String table) {
		PropDataContext requestContext = new PropDataContext();

		requestContext.setProperty(
				PropDataKey.CommandIdsKey.CREATED_BY.getValue(),
				"propdata@lattice-engines.com");

		requestContext.setProperty(
				PropDataKey.CommandsKey.COMMAND_NAME.getValue(),
				"RunMatchWithLEUniverse");
		requestContext.setProperty(
				PropDataKey.CommandsKey.CONTRACT_EXTERNAL_ID.getValue(),
				customer);
		requestContext.setProperty(
				PropDataKey.CommandsKey.DEPLOYMENT_EXTERNAL_ID.getValue(),
				customer);
		requestContext.setProperty(
				PropDataKey.CommandsKey.DESTTABLES.getValue(),
				"DerivedColumns|Experian_Source");

		requestContext.setProperty(
				PropDataKey.CommandsKey.IS_DOWNLOADING.getValue(),
				Boolean.FALSE);

		requestContext.setProperty(
				PropDataKey.CommandsKey.MAX_NUMR_ETRIES.getValue(), 5);
		requestContext.setProperty(
				PropDataKey.CommandsKey.SOURCE_TABLE.getValue(), table);

		return requestContext;
	}

	private PropDataContext getRequestContextForImport(String customer,
			String table, String keyCols, PropDataContext requestContext) {

		if (requestContext == null) {
			requestContext = new PropDataContext();
		}
		requestContext.setProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), customer);
		requestContext.setProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), table);
		requestContext.setProperty(
				PropDataKey.ImportExportKey.KEY_COLS.getValue(), keyCols);

		return requestContext;
	}

	private PropDataContext getRequestContextForExport(String customer,
			String table) {
		PropDataContext requestContext = new PropDataContext();
		requestContext.setProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), customer);
		requestContext.setProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), table);

		return requestContext;
	}

}
