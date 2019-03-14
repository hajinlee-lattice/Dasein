package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

public class ProductMapperFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final String PRODUCT_TABLE = "ProductTable";
    private static final String PRODUCT_DIR = "/tmp/ProductMapperFlowTestNG/product/";

    private String[] randomUUIDs = { UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            UUID.randomUUID().toString(), UUID.randomUUID().toString() };

    private String[] productFields = new String[] { "ProductId", "ProductName", "Description", "ProductType",
            "ProductBundle", "ProductLine", "ProductFamily", "ProductCategory", "ProductBundleId", "ProductLineId",
            "ProductFamilyId", "ProductCategoryId", "ProductStatus" };

    private Object[][] inputProductData = new Object[][] {

            // cases of products having differnet ids and same types
            { "1", "sku1", "Soap", ProductType.Hierarchy.name(), null, null, "Family1", "Category1", null, null,
                    randomUUIDs[0], null, ProductStatus.Active.name() },
            { "2", "sku2", "Dishwasher", ProductType.Hierarchy.name(), null, "Line1", "Family1", "Category1", null,
                    randomUUIDs[1], null, null, ProductStatus.Active.name() },

            // cases of products having same Ids but different types
            { "3", "sku3", "Pencil", ProductType.Hierarchy.name(), null, "Line3", "Family3", "Category3", null,
                    randomUUIDs[2], null, null, ProductStatus.Active.name() },
            { "3", "sku4", "Mouse", ProductType.Bundle.name(), "Bundle1", null, null, null, randomUUIDs[3], null, null,
                    null, ProductStatus.Active.name() },
            { "3", "sku5", "Monitor", ProductType.Bundle.name(), "Bundle2", null, null, null, randomUUIDs[4], null,
                    null, null, ProductStatus.Active.name() },

            // cases of products having different ids and types
            { "4", "sku6", "T-shirt", ProductType.Analytic.name(), null, "Line999", "Family999", "Category999", null,
                    randomUUIDs[5], null, null, ProductStatus.Active.name() },
            { "5", "sku7", "Printer", ProductType.Spending.name(), "Bundle999", null, null, null, randomUUIDs[6], null,
                    null, null, ProductStatus.Active.name() } };

    private String[] transactionFields = new String[] { "TransactionId", "TransactionTime", "TransactionType",
            "AccountId", "Amount", "ExtensionAttr1", "Quantity", "ContactId", "OrderId", "ProductId" };

    private Object[][] inputTransactionData = new Object[][] {
            // cases of transactions having productIds in productList
            { 1, 1502755200000L, "PurchaseHistory", 1, 10.0, "Ext1", 1, "Contact1", "Order1", "1" },
            { 2, 1502755200000L, "PurchaseHistory", 2, 10.0, "Ext1", 1, "Contact1", "Order1", "1" },
            { 3, 1503001576000L, "PurchaseHistory", 3, 10.0, "Ext2", 1, "Contact1", "Order1", "2" },
            { 4, 1503001577000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", "2" },
            { 5, 1503001578000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", "2" },
            { 6, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "Contact1", "Order1", "3" },
            { 7, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "Contact1", "Order1", "3" },
            { 8, 1503001576000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "4" },
            { 9, 1503001578000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "5" },

            // cases of transactions having productIds not in productList (a
            // dummy product will be generated instead)
            { 10, 1503001579000L, "PurchaseHistory", 5, 999.0, "Ext888", 1, "Contact888", "Order888", "888" },
            { 11, 1503001579000L, "PurchaseHistory", 5, 999.0, "Ext999", 1, "Contact999", "Order999", "999" } };

    private Object[][] expectedTransactionData = new Object[][] {
            { 1, 1502755200000L, "PurchaseHistory", 1, 10.0, "Ext1", 1, "Contact1", "Order1", randomUUIDs[0],
                    ProductType.Spending.name() },
            { 2, 1502755200000L, "PurchaseHistory", 2, 10.0, "Ext1", 1, "Contact1", "Order1", randomUUIDs[0],
                    ProductType.Spending.name() },
            { 3, 1503001576000L, "PurchaseHistory", 3, 10.0, "Ext2", 1, "Contact1", "Order1", randomUUIDs[1],
                    ProductType.Spending.name() },
            { 4, 1503001577000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", randomUUIDs[1],
                    ProductType.Spending.name() },
            { 5, 1503001578000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", randomUUIDs[1],
                    ProductType.Spending.name() },
            { 6, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "Contact1", "Order1", randomUUIDs[2],
                    ProductType.Spending.name() },
            { 6, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "Contact1", "Order1", randomUUIDs[3],
                    ProductType.Analytic.name() },
            { 6, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "Contact1", "Order1", randomUUIDs[4],
                    ProductType.Analytic.name() },
            { 7, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "Contact1", "Order1", randomUUIDs[2],
                    ProductType.Spending.name() },
            { 7, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "Contact1", "Order1", randomUUIDs[3],
                    ProductType.Analytic.name() },
            { 7, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "Contact1", "Order1", randomUUIDs[4],
                    ProductType.Analytic.name() },
            { 8, 1503001576000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "4",
                    ProductType.Analytic.name() },
            { 9, 1503001578000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "5",
                    ProductType.Spending.name() },
            { 10, 1503001579000L, "PurchaseHistory", 5, 999.0, "Ext888", 1, "Contact888", "Order888",
                    Product.UNKNOWN_PRODUCT_ID, ProductType.Spending.name() },
            { 11, 1503001579000L, "PurchaseHistory", 5, 999.0, "Ext999", 1, "Contact999", "Order999",
                    Product.UNKNOWN_PRODUCT_ID, ProductType.Spending.name() } };

    @Override
    protected String getFlowBeanName() {
        return ProductMapperFlow.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() {
        TransformationFlowParameters parameters = prepareInputData();
        executeDataFlow(parameters);
        verifyWithAvro();
    }

    @Override
    protected Table executeDataFlow() {
        return super.executeDataFlow();
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(PRODUCT_TABLE, PRODUCT_DIR + PRODUCT_TABLE + ".avro");
    }

    private TransformationFlowParameters prepareInputData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(transactionFields[0], Integer.class));
        columns.add(Pair.of(transactionFields[1], Long.class));
        columns.add(Pair.of(transactionFields[2], String.class));
        columns.add(Pair.of(transactionFields[3], Integer.class));
        columns.add(Pair.of(transactionFields[4], Double.class));
        columns.add(Pair.of(transactionFields[5], String.class));
        columns.add(Pair.of(transactionFields[6], Integer.class));
        columns.add(Pair.of(transactionFields[7], String.class));
        columns.add(Pair.of(transactionFields[8], String.class));
        columns.add(Pair.of(transactionFields[9], String.class));
        uploadDataToSharedAvroInput(inputTransactionData, columns);

        columns = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            columns.add(Pair.of(productFields[i], String.class));
        }
        uploadAvro(inputProductData, columns, PRODUCT_TABLE, PRODUCT_DIR);

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, PRODUCT_TABLE));
        parameters.setConfJson(JsonUtils.serialize(getConfiguration()));
        return parameters;
    }

    private ProductMapperConfig getConfiguration() {
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());
        return config;
    }

    private void verifyWithAvro() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), expectedTransactionData.length);
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            Object[] expectedRecord = expectedTransactionData[i];

            Integer transactionId = (Integer) record.get("TransactionId");
            Long transactionTime = (Long) record.get("TransactionTime");
            String productId = String.valueOf(record.get(InterfaceName.ProductId.name()));
            String productType = String.valueOf(record.get(InterfaceName.ProductType.name()));

            Assert.assertEquals(transactionId, expectedRecord[0]);
            Assert.assertEquals(transactionTime, expectedRecord[1]);
            Assert.assertEquals(productId, expectedRecord[expectedRecord.length - 2]);
            Assert.assertEquals(productType, expectedRecord[expectedRecord.length - 1]);
        }
    }
}
