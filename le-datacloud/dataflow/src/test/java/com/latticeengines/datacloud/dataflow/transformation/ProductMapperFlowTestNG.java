package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;
import java.util.Arrays;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.util.ProductUtils;

public class ProductMapperFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private String[] transactionFields = new String[] {
            "TransactionId", "TransactionTime", "TransactionType", "AccountId", "Amount", "ExtensionAttr1", "Quantity",
            "ContactId", "OrderId", "ProductId"
    };

    private Object[][] transactionData = new Object[][] {
            { 1, 1502755200000L, "PurchaseHistory", 1, 10.0, "Ext1", 1, "Contact1", "Order1", "1" },
            { 2, 1502755200000L, "PurchaseHistory", 2, 10.0, "Ext1", 1, "Contact1", "Order1", "1" },
            { 3, 1503001576000L, "PurchaseHistory", 3, 10.0, "Ext2", 1, "Contact1", "Order1", "2" },
            { 4, 1503001577000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", "2" },
            { 5, 1503001578000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "Contact1", "Order1", "2" },
            { 6, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "Contact1", "Order1", "3" },
            { 7, 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "Contact1", "Order1", "3" },
            { 8, 1503001576000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "4" },
            { 9, 1503001578000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "Contact1", "Order1", "5" }
    };

    private List<Product> productList = Arrays.asList(
            new Product("1", "sku1", "Soap", ProductType.Hierarchy.name(),
                    null, null, "Family1", "Category1",
                    null, null, UUID.randomUUID().toString(), null, ProductStatus.Active.name()),
            new Product("2", "sku2", "Dishwasher", ProductType.Hierarchy.name(),
                    null, "Line1", "Family1", "Category1",
                    null, UUID.randomUUID().toString(), null, null, ProductStatus.Active.name()),
            new Product("3", "sku3", "Pencil", ProductType.Hierarchy.name(),
                    null, "Line3", "Family3", "Category3",
                    null, UUID.randomUUID().toString(), null, null, ProductStatus.Active.name()),
            new Product("3", "sku4", "Mouse", ProductType.Bundle.name(),
                    "Bundle1", null, null, null,
                    UUID.randomUUID().toString(), null, null, null, ProductStatus.Active.name()),
            new Product("3", "sku5", "Monitor", ProductType.Bundle.name(),
                    "Bundle2", null, null, null,
                    UUID.randomUUID().toString(), null, null, null, ProductStatus.Active.name()),
            new Product("4", "sku6", "T-shirt", ProductType.Analytic.name(),
                    null, "Line999", "Family999", "Category999",
                    null, UUID.randomUUID().toString(), null, null, ProductStatus.Active.name()),
            new Product("5", "sku7", "Printer", ProductType.Spending.name(),
                    "Bundle999", null, null, null,
                    UUID.randomUUID().toString(), null, null, null, ProductStatus.Active.name())
    );

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
        uploadDataToSharedAvroInput(transactionData, columns);

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setConfJson(JsonUtils.serialize(getConfiguration()));
        return parameters;
    }

    private ProductMapperConfig getConfiguration() {
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());
        config.setProductMap(ProductUtils.getActiveProductMap(productList));
        return config;
    }

    private void verifyWithAvro() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 13);
        for (GenericRecord record: records) {
            Integer transactionId = Integer.valueOf(record.get("TransactionId").toString());
            String productId = String.valueOf(record.get(InterfaceName.ProductId.name()));
            String productType = String.valueOf(record.get(InterfaceName.ProductType.name()));

            if (transactionId >= 1 && transactionId <= 2) {
                Assert.assertTrue(productId.equals(productList.get(0).getProductFamilyId()));
                Assert.assertTrue(productType.equals(ProductType.Spending.name()));
            } else if (transactionId >= 3 && transactionId <= 5) {
                Assert.assertTrue(productId.equals(productList.get(1).getProductLineId()));
                Assert.assertTrue(productType.equals(ProductType.Spending.name()));
            } else if (transactionId >= 6 && transactionId <= 7) {
                Assert.assertTrue(productId.equals(productList.get(2).getProductLineId()) ||
                        productId.equals(productList.get(3).getProductBundleId()) ||
                        productId.equals(productList.get(4).getProductBundleId()));
                Assert.assertTrue(productType.equals(ProductType.Spending.name()) ||
                        productType.equals(ProductType.Analytic.name()));
            } else if (transactionId == 8) {
                Assert.assertTrue(productId.equals(productList.get(5).getProductId()));
                Assert.assertTrue(productType.equals(ProductType.Analytic.name()));
            } else if (transactionId == 9) {
                Assert.assertTrue(productId.equals(productList.get(6).getProductId()));
                Assert.assertTrue(productType.equals(ProductType.Spending.name()));
            }
        }
    }
}
