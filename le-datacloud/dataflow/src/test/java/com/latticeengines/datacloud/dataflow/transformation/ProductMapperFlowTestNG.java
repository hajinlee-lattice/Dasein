package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Arrays;

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

public class ProductMapperFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private String[] rawTrxFields = new String[] {
            "TransactionTime", "TransactionType", "AccountId", "Amount", "ExtensionAttr1",
            "Quantity", "ProductId", "ContactId", "OrderId", "TransactionId"
    };

    private Object[][] rawTrxData = new Object[][] {
            { 1502755200000L, "PurchaseHistory", 1, 10.0, "Ext1", 1, "1", "Contact1", "Order1", 1 },
            { 1502755200000L, "PurchaseHistory", 2, 10.0, "Ext1", 1, "1", "Contact1", "Order1", 2 },
            { 1503001576000L, "PurchaseHistory", 3, 10.0, "Ext2", 1, "1", "Contact1", "Order1", 3 },
            { 1503001577000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "1", "Contact1", "Order1", 4 },
            { 1503001578000L, "PurchaseHistory", 3, 10.0, "Ext3", 1, "1", "Contact1", "Order1", 5 },
            { 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext1", 1, "2", "Contact1", "Order1", 6 },
            { 1502755200000L, "PurchaseHistory", 4, 10.0, "Ext2", 1, "2", "Contact1", "Order1", 7 },
            { 1503001576000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "2", "Contact1", "Order1", 8 },
            { 1503001578000L, "PurchaseHistory", 4, 10.0, "Ext3", 1, "3", "Contact1", "Order1", 9 }
    };

    private Map<String, List<Product>> getProductMap() {
        Map<String, List<Product>> productMap = new HashMap<>();
        productMap.put("1", Arrays.asList(
                createProduct("1", "sku1", "b1", null, null, null),
                createProduct("1", "sku1", "b2", null, null, null),
                createProduct("1", "sku1", null, "pl1", "pf1", "pc1")));
        productMap.put("3", Collections.singletonList(createProduct("3", "sku3", "b1", "pl1", "pf1", "pc1")));
        productMap.put("4", Collections.singletonList(createProduct("4", "sku4", null, null, null, null)));
        productMap.put("5", Collections.singletonList(createProduct("5", "sku5", "b3", "pl1", "pf1", "pc1")));

        return productMap;
    }

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
        columns.add(Pair.of(rawTrxFields[0], Long.class));
        columns.add(Pair.of(rawTrxFields[1], String.class));
        columns.add(Pair.of(rawTrxFields[2], Integer.class));
        columns.add(Pair.of(rawTrxFields[3], Double.class));
        columns.add(Pair.of(rawTrxFields[4], String.class));
        columns.add(Pair.of(rawTrxFields[5], Integer.class));
        columns.add(Pair.of(rawTrxFields[6], String.class));
        columns.add(Pair.of(rawTrxFields[7], String.class));
        columns.add(Pair.of(rawTrxFields[8], String.class));
        columns.add(Pair.of(rawTrxFields[9], Integer.class));
        uploadDataToSharedAvroInput(rawTrxData, columns);

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setConfJson(JsonUtils.serialize(getProductMapperConfig()));
        return parameters;
    }

    private ProductMapperConfig getProductMapperConfig() {
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());
        config.setProductMap(getProductMap());

        return config;
    }

    private Product createProduct(String id, String name, String bundle, String productLine, String productFamily,
                                  String productCategory) {
        Product product = new Product();
        product.setProductId(id);
        product.setProductName(name);
        product.setProductBundle(bundle);
        product.setProductLine(productLine);
        product.setProductFamily(productFamily);
        product.setProductCategory(productCategory);
        return product;
    }

    private void verifyWithAvro() {
        List<GenericRecord> records = readOutput();

        Assert.assertEquals(records.size(), 17);
        for (GenericRecord record: records) {
            int trxId = Integer.valueOf(String.valueOf(record.get(InterfaceName.TransactionId.name())));
            String productId = String.valueOf(record.get(InterfaceName.ProductId.name()));
            String productType = String.valueOf(record.get(InterfaceName.ProductType.name()));
            String contactId = String.valueOf(record.get(InterfaceName.ContactId.name()));

            Assert.assertTrue(contactId.equals("Contact1"));

            if (trxId >= 1 && trxId <= 5) {
                Assert.assertTrue(productId.equals("b1") || productId.equals("b2") || productId.equals("pl1"));
                if (productId.equals("b1") || productId.equals("b2")) {
                    Assert.assertTrue(productType.equals(ProductType.ANALYTIC.getName()));
                } else if (productId.equals("pl1")) {
                    Assert.assertTrue(productType.equals(ProductType.SPENDING.getName()));
                }
            }

            Assert.assertTrue(trxId != 6 && trxId != 7 && trxId != 8);

            if (trxId == 9) {
                Assert.assertTrue(productId.equals("b1") || productId.equals("pl1"));
                if (productId.equals("b1")) {
                    Assert.assertTrue(productType.equals(ProductType.ANALYTIC.getName()));
                } else if (productId.equals("pl1")) {
                    Assert.assertTrue(productType.equals(ProductType.SPENDING.getName()));
                }
            }
        }
    }
}
