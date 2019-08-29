package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTransactionExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class OrphanTransactionExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(OrphanTransactionExportFlowTestNG.class);

    private static final String ACCOUNT_TABLE = "AccountTable";
    private static final String PRODUCT_TABLE = "ProductTable";
    private static final String TRANSACTION_TABLE = "TransactionTable";
    private static final String ACCOUNT_DIR = "/tmp/OrphanTransactionExportFlowTestNG/account/";
    private static final String PRODUCT_DIR = "/tmp/OrphanTransactionExportFlowTestNG/product/";
    private static final String TRANSACTION_DIR = "/tmp/OrphanTransactionExportFlowTestNG/transaction/";
    private static final String[] TXN_ATTRS = new String[] { InterfaceName.TransactionId.name(),
            InterfaceName.AccountId.name(), InterfaceName.ProductId.name(), InterfaceName.TransactionCount.name() };
    private static final String[] TXN_NOAID_ATTRS = new String[] { InterfaceName.TransactionId.name(),
            InterfaceName.ProductId.name(), InterfaceName.TransactionCount.name() };

    private Object[][] accountData = new Object[][] {
            // "AccountId", "Name"
            { "A001", "Husky" }, //
            { "A002", "Alaskan Malamute" }, //
            { "A003", "Collie" }, //
            { "A004", "Chihuahua" }, //
            { "A005", "Labrador Retriever" } //
    };

    private Object[][] productData = new Object[][] {
            // "ProductId", "ProductName"
            { "P0001", "test_product_1" }, //
            { "P0002", "test_product_2" }, //
            { "P0003", "test_product_3" }, //
            { "P0004", "test_product_4" }, //
            { "P0004", "test_product_4" }, //
            { "P0005", "test_product_5" } //
    };

    private Object[][] transactionData = new Object[][] {
            // "TransactionId", "AccountId", "ProductId", "TransactionCount"
            { "T00200", "A001", "P0002", 200L }, //
            { "T01234", "A005", "P0010", 200L }, //
            { "T06666", "A010", "P0088", 300L }, //
            { "T08080", "A004", "P0003", 150L }, //
            { "T18888", "A006", "P0004", 998L } //
    };

    private Object[][] expectedData = new Object[][] {
            // "TransactionId", "AccountId", "ProductId", "TransactionCount"
            { "T18888", "A006", "P0004", 998L }, //
            { "T01234", "A005", "P0010", 200L }, //
            { "T06666", "A010", "P0088", 300L }
    };

    private Object[][] expectedNoAIDData = new Object[][] {
            // "TransactionId", "ProductId", "TransactionCount"
            { "T18888", "P0004", 998L }, //
            { "T01234", "P0010", 200L }, //
            { "T06666", "P0088", 300L } //
    };

    private Object[][] expectNullCaseData = new Object[][] {
            // "TransactionId", "AccountId", "ProductId", "TransactionCount"
            { "T00200", "A001", "P0002", 200L }, //
            { "T01234", "A005", "P0010", 200L }, //
            { "T06666", "A010", "P0088", 300L }, //
            { "T08080", "A004", "P0003", 150L }, //
            { "T18888", "A006", "P0004", 998L } //
    };

    @BeforeClass(groups = "functional")
    public void setupOrphanTransactionExportFlowTestNG() {
        uploadAvro(accountData, prepareAccountData(), ACCOUNT_TABLE, ACCOUNT_DIR);
        uploadAvro(productData, prepareProductData(), PRODUCT_TABLE, PRODUCT_DIR);
        uploadAvro(transactionData, prepareTxnData(), TRANSACTION_TABLE, TRANSACTION_DIR);
    }

    @Test(groups = "functional")
    public void testOrphanTransactionExportFlow() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, productData, transactionData, null);
        executeDataFlow(parameters);
        verifyResult(expectedData, 3, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testNullAccountTable() {
        OrphanTransactionExportParameters parameters = prepareInput(null, productData, transactionData, null);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData, 5, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testNullProductTable() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, null, transactionData, null);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData, 5, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testOrphanTransactionNoAIDExportFlow() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, productData, transactionData,
                new ArrayList<>());
        executeDataFlow(parameters);
        verifyResult(expectedNoAIDData, 3, TXN_NOAID_ATTRS);
    }

    @Override
    protected String getFlowBeanName() {
        return OrphanTransactionExportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(
                ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro",
                PRODUCT_TABLE, PRODUCT_DIR + PRODUCT_TABLE + ".avro",
                TRANSACTION_TABLE, TRANSACTION_DIR + TRANSACTION_TABLE + ".avro");
    }

    private OrphanTransactionExportParameters prepareInput(Object[][] accountData, Object[][] productData,
            Object[][] transactionData, List<String> validatedColumns) {
        OrphanTransactionExportParameters parameters = new OrphanTransactionExportParameters();
        if (accountData != null) {
            uploadAvro(accountData, prepareAccountData(), ACCOUNT_TABLE, ACCOUNT_DIR);
            parameters.setAccountTable(ACCOUNT_TABLE);
        }
        if (productData != null){
            uploadAvro(productData, prepareProductData(), PRODUCT_TABLE, PRODUCT_DIR);
            parameters.setProductTable(PRODUCT_TABLE);
        }

        uploadAvro(transactionData, prepareTxnData(), TRANSACTION_TABLE, TRANSACTION_DIR);
        parameters.setTransactionTable(TRANSACTION_TABLE);
        if (validatedColumns == null) {
            // Export attributes passed in is only for Account & Contact entity,
            // don't include Transaction attributes
            parameters.setValidatedColumns(Arrays.asList(InterfaceName.AccountId.name()));
        } else {
            parameters.setValidatedColumns(validatedColumns);
        }

        return parameters;
    }

    private void verifyResult(Object[][] expectedData, int expectNumOfRows, String[] attributes) {
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            log.info(record.toString());
            for (int i = 0; i < attributes.length; i++) {
                Assert.assertTrue(isObjEquals(record.get(attributes[i]), expectedData[rowNum][i]));
            }
            rowNum ++;
        }
        Assert.assertEquals(rowNum, expectNumOfRows);
    }

    private List<Pair<String, Class<?>>> prepareAccountData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Name.name(), String.class));
        return columns;
    }

    private List<Pair<String, Class<?>>> prepareProductData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductName.name(), String.class));
        return columns;
    }

    private List<Pair<String, Class<?>>> prepareTxnData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.TransactionId.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        columns.add(Pair.of(InterfaceName.TransactionCount.name(), Long.class));
        return columns;
    }
}
