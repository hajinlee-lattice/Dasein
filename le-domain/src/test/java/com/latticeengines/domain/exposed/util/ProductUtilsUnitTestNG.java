package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;

@Component
public class ProductUtilsUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(ProductUtilsUnitTestNG.class);
    private YarnConfiguration yarnConfiguration;
    private final String PATH = "/tmp/ProductUtilsTestNG/";
    private List<Product> productList;
    private List<Product> loadedProductList;

    @BeforeClass
    public void setup() throws IOException {
        yarnConfiguration = new YarnConfiguration();

        if (HdfsUtils.fileExists(yarnConfiguration, PATH)) {
            HdfsUtils.rmdir(yarnConfiguration, PATH);
        }
        HdfsUtils.mkdir(yarnConfiguration, PATH);
        log.info("Test artifacts are in " + PATH);

        Product desktopGadgets = new Product();
        desktopGadgets.setProductId("bundle1");
        desktopGadgets.setProductName("Desktop gadgets bundle #1");
        desktopGadgets.setProductDescription("A bunch of desktop gadgets.");
        desktopGadgets.setProductBundle("Bundle #1");
        desktopGadgets.setProductType(ProductType.Bundle.name());
        desktopGadgets.setProductStatus(ProductStatus.Active.name());

        Product soap = new Product();
        soap.setProductId("sku1");
        soap.setProductName("Soap");
        soap.setProductDescription("Soap with heavy foams.");
        soap.setProductLine("Detergent line #1");
        soap.setProductFamily("Detergent family #1");
        soap.setProductCategory("Detergent category #1");
        soap.setProductType(ProductType.Hierarchy.name());
        soap.setProductStatus(ProductStatus.Active.name());

        Product dishwasher = new Product();
        dishwasher.setProductId("sku2");
        dishwasher.setProductName("Dishwasher");
        dishwasher.setProductDescription("Heavy-duty dishwasher.");
        dishwasher.setProductLine("Appliance Line #2");
        dishwasher.setProductFamily("Appliance family #2");
        dishwasher.setProductCategory("Appliance category #2");
        dishwasher.setProductType(ProductType.Hierarchy.name());
        dishwasher.setProductStatus(ProductStatus.Active.name());

        Product desktopGadgetsAnalytic = new Product();
        desktopGadgetsAnalytic.setProductId("bundle1");
        desktopGadgetsAnalytic.setProductName("Desktop gadgets bundle #1");
        desktopGadgetsAnalytic.setProductDescription("A bunch of desktop gadgets.");
        desktopGadgetsAnalytic.setProductBundle("Bundle #1");
        desktopGadgetsAnalytic.setProductType(ProductType.Analytic.name());
        desktopGadgetsAnalytic.setProductStatus(ProductStatus.Active.name());

        Product soapSpending = new Product();
        soapSpending.setProductId("sku1");
        soapSpending.setProductName("Soap");
        soapSpending.setProductDescription("Soap with heavy foams.");
        soapSpending.setProductLine("Detergent line #1");
        soapSpending.setProductFamily("Detergent family #1");
        soapSpending.setProductCategory("Detergent category #1");
        soapSpending.setProductType(ProductType.Spending.name());
        soapSpending.setProductStatus(ProductStatus.Active.name());

        Product dishwasherSpending = new Product();
        dishwasherSpending.setProductId("sku2");
        dishwasherSpending.setProductName("Dishwasher");
        dishwasherSpending.setProductDescription("Heavy-duty dishwasher.");
        dishwasherSpending.setProductLine("Appliance line #2");
        dishwasherSpending.setProductFamily("Appliance family #2");
        dishwasherSpending.setProductCategory("Appliance category #2");
        dishwasherSpending.setProductType(ProductType.Spending.name());
        dishwasherSpending.setProductStatus(ProductStatus.Active.name());

        productList = Arrays.asList(desktopGadgets, soap, dishwasher,
                desktopGadgetsAnalytic, soapSpending, dishwasherSpending);
    }

    @AfterClass
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, PATH);
        log.info(String.format("Test artifacts in %s are removed.", PATH));
    }

    @Test(groups = "unit")
    public void testSaveProducts() throws IOException {
        ProductUtils.saveProducts(yarnConfiguration, PATH, productList);
        Assert.assertEquals(productList.size(), 6);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, PATH));
    }

    @Test(groups = "unit", dependsOnMethods = "testSaveProducts")
    public void testLoadProducts() {
        loadedProductList = ProductUtils.loadProducts(yarnConfiguration, PATH);
        Assert.assertEquals(loadedProductList.size(), productList.size());
        Assert.assertEquals(loadedProductList.get(0).getProductId(), "bundle1");
        Assert.assertEquals(loadedProductList.get(0).getProductType(), ProductType.Bundle.name());
        Assert.assertEquals(loadedProductList.get(0).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(loadedProductList.get(0).getProductBundle(), "Bundle #1");
        Assert.assertEquals(loadedProductList.get(0).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(1).getProductId(), "sku1");
        Assert.assertEquals(loadedProductList.get(1).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(loadedProductList.get(1).getProductName(), "Soap");
        Assert.assertEquals(loadedProductList.get(1).getProductLine(), "Detergent line #1");
        Assert.assertEquals(loadedProductList.get(1).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(2).getProductId(), "sku2");
        Assert.assertEquals(loadedProductList.get(2).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(loadedProductList.get(2).getProductName(), "Dishwasher");
        Assert.assertEquals(loadedProductList.get(2).getProductFamily(), "Appliance family #2");
        Assert.assertEquals(loadedProductList.get(2).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(3).getProductId(), "bundle1");
        Assert.assertEquals(loadedProductList.get(3).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(loadedProductList.get(3).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(loadedProductList.get(3).getProductBundle(), "Bundle #1");
        Assert.assertEquals(loadedProductList.get(3).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(4).getProductId(), "sku1");
        Assert.assertEquals(loadedProductList.get(4).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(loadedProductList.get(4).getProductName(), "Soap");
        Assert.assertEquals(loadedProductList.get(4).getProductCategory(), "Detergent category #1");
        Assert.assertEquals(loadedProductList.get(4).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(5).getProductId(), "sku2");
        Assert.assertEquals(loadedProductList.get(5).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(loadedProductList.get(5).getProductName(), "Dishwasher");
        Assert.assertEquals(loadedProductList.get(5).getProductCategory(), "Appliance category #2");
        Assert.assertEquals(loadedProductList.get(5).getProductStatus(), ProductStatus.Active.name());
    }

    @Test(groups = "unit", dependsOnMethods = "testLoadProducts")
    public void testGetProductMap() {
        Map<String, List<Product>> productMap = ProductUtils.getProductMap(loadedProductList,
                ProductType.Analytic.name(), ProductType.Spending.name());
        Assert.assertEquals(productMap.size(), 3);
        Assert.assertEquals(productMap.get("sku1").size(), 1);
        Assert.assertEquals(productMap.get("sku1").get(0).getProductName(), "Soap");
        Assert.assertEquals(productMap.get("bundle1").size(), 1);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(productMap.get("sku2").size(), 1);
        Assert.assertEquals(productMap.get("sku2").get(0).getProductName(), "Dishwasher");

        productMap = ProductUtils.getProductMap(loadedProductList, ProductType.Analytic.name());
        Assert.assertEquals(productMap.size(), 1);
        Assert.assertEquals(productMap.get("bundle1").size(), 1);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductName(), "Desktop gadgets bundle #1");

        productMap = ProductUtils.getProductMap(loadedProductList, ProductType.Spending.name());
        Assert.assertEquals(productMap.size(), 2);
        Assert.assertEquals(productMap.get("sku1").size(), 1);
        Assert.assertEquals(productMap.get("sku1").get(0).getProductName(), "Soap");
        Assert.assertEquals(productMap.get("sku2").size(), 1);
        Assert.assertEquals(productMap.get("sku2").get(0).getProductName(), "Dishwasher");
    }

    @Test(groups = "unit", dependsOnMethods = "testLoadProducts")
    public void testGetActiveProductMap() {
        Map<String, List<Product>> productMap = ProductUtils.getActiveProductMap(loadedProductList);
        Assert.assertEquals(productMap.size(), 3);
        Assert.assertEquals(productMap.get("sku1").size(), 2);
        Assert.assertEquals(productMap.get("sku1").get(0).getProductName(), "Soap");
        Assert.assertEquals(productMap.get("sku1").get(0).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(productMap.get("sku1").get(1).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(productMap.get("bundle1").size(), 2);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductType(), ProductType.Bundle.name());
        Assert.assertEquals(productMap.get("bundle1").get(1).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(productMap.get("sku2").size(), 2);
        Assert.assertEquals(productMap.get("sku2").get(0).getProductName(), "Dishwasher");
        Assert.assertEquals(productMap.get("sku2").get(0).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(productMap.get("sku2").get(1).getProductType(), ProductType.Spending.name());

        loadedProductList.get(0).setProductStatus(ProductStatus.Obsolete.name());
        productMap = ProductUtils.getActiveProductMap(loadedProductList);
        Assert.assertEquals(productMap.size(), 3);
        Assert.assertEquals(productMap.get("bundle1").size(), 1);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductType(), ProductType.Analytic.name());
        loadedProductList.get(0).setProductStatus(ProductStatus.Active.name());
    }

    @Test(groups = "unit", dependsOnMethods = "testLoadProducts")
    public void testGetActiveProductMapByTypes() {
        String[] types = { ProductType.Analytic.name(), ProductType.Spending.name() };
        Map<String, List<Product>> productMap = ProductUtils.getActiveProductMap(loadedProductList, types);
        Assert.assertEquals(productMap.size(), 3);
        Assert.assertEquals(productMap.get("sku1").size(), 1);
        Assert.assertEquals(productMap.get("sku1").get(0).getProductName(), "Soap");
        Assert.assertEquals(productMap.get("bundle1").size(), 1);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(productMap.get("sku2").size(), 1);
        Assert.assertEquals(productMap.get("sku2").get(0).getProductName(), "Dishwasher");
    }

    @Test(groups = "unit", dependsOnMethods = "testLoadProducts")
    public void testGetProductMapByCompositeId() {
        Map<String, Product> productMap = ProductUtils.getProductMapByCompositeId(loadedProductList);
        Assert.assertEquals(productMap.size(), 6);
        Assert.assertTrue(productMap.keySet().contains("Bundle__bundle1__Bundle #1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku2"));
        Assert.assertTrue(productMap.keySet().contains("Analytic__Desktop gadgets bundle #1"));
        Assert.assertTrue(productMap.keySet().contains("Spending__Soap"));
        Assert.assertTrue(productMap.keySet().contains("Spending__Dishwasher"));

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Active.name());
        Assert.assertEquals(productMap.size(), 6);

        loadedProductList.get(1).setProductStatus(ProductStatus.Obsolete.name());

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Active.name());
        Assert.assertEquals(productMap.size(), 5);
        Assert.assertTrue(productMap.keySet().contains("Bundle__bundle1__Bundle #1"));
        Assert.assertFalse(productMap.keySet().contains("Hierarchy__sku1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku2"));
        Assert.assertTrue(productMap.keySet().contains("Analytic__Desktop gadgets bundle #1"));
        Assert.assertTrue(productMap.keySet().contains("Spending__Soap"));
        Assert.assertTrue(productMap.keySet().contains("Spending__Dishwasher"));

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Obsolete.name());
        Assert.assertEquals(productMap.size(), 1);

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList,
                ProductStatus.Active.name(), ProductStatus.Obsolete.name());
        Assert.assertEquals(productMap.size(), 6);

        loadedProductList.get(1).setProductStatus(ProductStatus.Active.name());
    }
}
