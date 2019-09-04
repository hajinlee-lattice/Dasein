package com.latticeengines.cdl.dataflow;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomTrxField;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionCount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionId;

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
    private static final String TRANSACTION_EM_TABLE = "TransactionEMTable";
    private static final String ACCOUNT_DIR = "/tmp/OrphanTransactionExportFlowTestNG/account/";
    private static final String PRODUCT_DIR = "/tmp/OrphanTransactionExportFlowTestNG/product/";
    private static final String TRANSACTION_DIR = "/tmp/OrphanTransactionExportFlowTestNG/transaction/legacy/";
    private static final String TRANSACTION_EM_DIR = "/tmp/OrphanTransactionExportFlowTestNG/transaction/em/";
    private static final String[] TXN_ATTRS = new String[] { //
            TransactionId.name(), AccountId.name(), ProductId.name(), TransactionCount.name() };
    private static final String[] TXN_NOAID_ATTRS = new String[] { //
            TransactionId.name(), ProductId.name(), TransactionCount.name() };

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

    private Object[][] transactionEMData = new Object[][] {
            // "TransactionId", "AccountId", "CustomerAccountId", "ProductId",
            // "TransactionCount", "CustomTrxField"
            { "T00200", "A001", "CA001", "P0002", 200L, "{111}" }, //
            { "T01234", "A005", "CA005", "P0010", 200L, "{222}" }, //
            { "T06666", "A010", "CA010", "P0088", 300L, "{333}" }, //
            { "T08080", "A004", "CA004", "P0003", 150L, "{444}" }, //
            { "T18888", "A006", "CA006", "P0004", 998L, "{555}" } //
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
        uploadAvro(accountData, getAccountSchema(), ACCOUNT_TABLE, ACCOUNT_DIR);
        uploadAvro(productData, getProductSchema(), PRODUCT_TABLE, PRODUCT_DIR);
        uploadAvro(transactionData, getTxnSchema(), TRANSACTION_TABLE, TRANSACTION_DIR);
        uploadAvro(transactionEMData, getTxnEMSchema(), TRANSACTION_EM_TABLE, TRANSACTION_EM_DIR);
    }

    @Test(groups = "functional")
    public void testOrphanTransactionExportFlow() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, productData, transactionData, null,
                false);
        executeDataFlow(parameters);
        verifyResult(expectedData, 3, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testNullAccountTable() {
        OrphanTransactionExportParameters parameters = prepareInput(null, productData, transactionData, null, false);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData, 5, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testNullProductTable() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, null, transactionData, null, false);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData, 5, TXN_ATTRS);
    }

    @Test(groups = "functional")
    public void testOrphanTransactionNoAIDExportFlow() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, productData, transactionData,
                new ArrayList<>(), false);
        executeDataFlow(parameters);
        verifyResult(expectedNoAIDData, 3, TXN_NOAID_ATTRS);
    }

    @Test(groups = "functional")
    public void testOrphanTransactionEMExportFlow() {
        OrphanTransactionExportParameters parameters = prepareInput(accountData, productData, transactionEMData, null,
                true);
        executeDataFlow(parameters);
        verifyResult(expectedData, 3, TXN_ATTRS);
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
                TRANSACTION_TABLE, TRANSACTION_DIR + TRANSACTION_TABLE + ".avro", TRANSACTION_EM_TABLE,
                TRANSACTION_EM_DIR + TRANSACTION_EM_TABLE + ".avro");
    }

    private OrphanTransactionExportParameters prepareInput(Object[][] accountData, Object[][] productData,
            Object[][] transactionData, List<String> validatedColumns, boolean entityMatchEnabled) {
        OrphanTransactionExportParameters parameters = new OrphanTransactionExportParameters();
        if (accountData != null) {
            uploadAvro(accountData, getAccountSchema(), ACCOUNT_TABLE, ACCOUNT_DIR);
            parameters.setAccountTable(ACCOUNT_TABLE);
        }
        if (productData != null){
            uploadAvro(productData, getProductSchema(), PRODUCT_TABLE, PRODUCT_DIR);
            parameters.setProductTable(PRODUCT_TABLE);
        }

        if (entityMatchEnabled) {
            uploadAvro(transactionData, getTxnEMSchema(), TRANSACTION_EM_TABLE, TRANSACTION_EM_DIR);
            parameters.setTransactionTable(TRANSACTION_EM_TABLE);
        } else {
            uploadAvro(transactionData, getTxnSchema(), TRANSACTION_TABLE, TRANSACTION_DIR);
            parameters.setTransactionTable(TRANSACTION_TABLE);
        }

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

    private List<Pair<String, Class<?>>> getAccountSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(AccountId.name(), String.class));
        columns.add(Pair.of(Name.name(), String.class));
        return columns;
    }

    private List<Pair<String, Class<?>>> getProductSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(ProductId.name(), String.class));
        columns.add(Pair.of(ProductName.name(), String.class));
        return columns;
    }

    // For legacy tenant
    private List<Pair<String, Class<?>>> getTxnSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(TransactionId.name(), String.class));
        columns.add(Pair.of(AccountId.name(), String.class));
        columns.add(Pair.of(ProductId.name(), String.class));
        columns.add(Pair.of(TransactionCount.name(), Long.class));
        return columns;
    }

    // For entity match tenant
    private List<Pair<String, Class<?>>> getTxnEMSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(TransactionId.name(), String.class));
        columns.add(Pair.of(AccountId.name(), String.class));
        columns.add(Pair.of(CustomerAccountId.name(), String.class));
        columns.add(Pair.of(ProductId.name(), String.class));
        columns.add(Pair.of(TransactionCount.name(), Long.class));
        columns.add(Pair.of(CustomTrxField.name(), String.class));
        return columns;
    }
}
