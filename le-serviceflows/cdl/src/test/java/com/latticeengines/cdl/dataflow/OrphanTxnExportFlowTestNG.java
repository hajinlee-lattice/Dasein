package com.latticeengines.cdl.dataflow;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTxnExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class OrphanTxnExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final String ACCOUNT_TABLE = "account_table";
    private static final String PRODUCT_TABLE = "product_table";
    private static final String TXN_TABLE = "txn_table";
    private static final String ACCOUNT_DIR = "/tmp/OrphanTxnExportFlowTestNG/account/";
    private static final String PRODUCT_DIR = "/tmp/OrphanTxnExportFlowTestNG/product/";
    private static final String TXN_DIR = "/tmp/OrphanTxnExportFlowTestNG/txn/";


    private Object[][] accountData = new Object[][]{
            //"AccountId", "Name"
            { "A001", "Husky" },
            { "A002", "Alaskan Malamute" },
            { "A003", "Collie" },
            { "A004", "Chihuahua" },
            { "A005", "Labrador Retriever" }
    };

    private Object[][] productData = new Object[][]{
            //"ProductId", "ProductName"
            { "P0001", "test_product_1" },
            { "P0002", "test_product_2" },
            { "P0003", "test_product_3" },
            { "P0004", "test_product_4" },
            { "P0005", "test_product_5" }
    };

    private Object[][] transactionData = new Object[][]{
            //"TransactionId", "AccountId", "ProductId", "TransactionCount"
            { "T00200", "A001", "P0002", 200L },
            { "T01234", "A005", "P0010", 200L },
            { "T06666", "A010", "P0088", 300L },
            { "T08080", "A004", "P0003", 150L },
            { "T18888", "A006", "P0004", 998L }
    };

    private Object[][] expectedData = new Object[][]{
            //"AccountId", "ProductId", "TransactionId", "TransactionCount"
            { "A006", "P0004", "T18888", 998L },
            { "A005", "P0010", "T01234", 200L },
            { "A010", "P0088", "T06666", 300L }
    };

    private Object[][] expectNullCaseData = new Object[][]{
            //"AccountId", "ProductId", "TransactionId", "TransactionCount"
            { "A001", "P0002", "T00200", 200L },
            { "A005", "P0010", "T01234", 200L },
            { "A010", "P0088", "T06666", 300L },
            { "A004", "P0003", "T08080", 150L },
            { "A006", "P0004", "T18888", 998L }
    };

    @Test(groups = "functional")
    public void testOrphanTxnExportFlow(){
        OrphanTxnExportParameters parameters = prepareInput(accountData,productData,transactionData);
        executeDataFlow(parameters);
        verifyResult(expectedData,3);
    }

    @Test(groups = "functional")
    public void testNullAccountTable(){
        OrphanTxnExportParameters parameters = prepareInput(null,productData,transactionData);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData,5);
    }

    @Test(groups = "functional")
    public void testNullProductTable(){
        OrphanTxnExportParameters parameters = prepareInput(accountData,null,transactionData);
        executeDataFlow(parameters);
        verifyResult(expectNullCaseData,5);
    }

    public OrphanTxnExportParameters prepareInput(Object[][] accountData,Object[][] productData,Object[][] transactionData){
        OrphanTxnExportParameters parameters = new OrphanTxnExportParameters();
        if (accountData != null){
            uploadAvro(accountData, prepareAccountData(), ACCOUNT_TABLE, ACCOUNT_DIR);
            parameters.setAccountTable(ACCOUNT_TABLE);
        }
        if (productData != null){
            uploadAvro(productData, prepareProductData(), PRODUCT_TABLE, PRODUCT_DIR);
            parameters.setProductTable(PRODUCT_TABLE);
        }
        uploadAvro(transactionData, prepareTxnData(), TXN_TABLE, TXN_DIR);
        parameters.setTxnTable(TXN_TABLE);

        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(
                ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro",
                PRODUCT_TABLE, PRODUCT_DIR + PRODUCT_TABLE + ".avro",
                TXN_TABLE, TXN_DIR + TXN_TABLE + ".avro"
        );
    }

    public void verifyResult(Object[][] expectedData,int expectNumOfRows){
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get(0).toString(),expectedData[rowNum][0]);
            Assert.assertEquals(record.get(1).toString(),expectedData[rowNum][1]);
            Assert.assertEquals(record.get(2).toString(),expectedData[rowNum][2]);
            Assert.assertEquals(record.get(3),expectedData[rowNum][3]);
            rowNum ++;
        }
        Assert.assertEquals(rowNum,expectNumOfRows);
    }

    private List<Pair<String, Class<?>>> prepareAccountData(){
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Name.name(), String.class));
        return columns;
    }

    private List<Pair<String, Class<?>>> prepareProductData(){
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

    @Override
    protected String getFlowBeanName() {
        return OrphanTxnExportFlow.DATAFLOW_BEAN_NAME;
    }

}
