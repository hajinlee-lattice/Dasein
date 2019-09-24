package com.latticeengines.serviceflows.dataflow;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.util.ProductUtils;

public class ProductUtilsFunctionalTestNG {
    private static final Logger log = LoggerFactory.getLogger(ProductUtilsFunctionalTestNG.class);
    private YarnConfiguration yarnConfiguration;
    private final String PATH = "/tmp/ProductUtilsTestNG/";
    private List<Product> productList;
    private List<Product> loadedProductList;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        yarnConfiguration = new YarnConfiguration();

        if (HdfsUtils.fileExists(yarnConfiguration, PATH)) {
            HdfsUtils.rmdir(yarnConfiguration, PATH);
        }
        HdfsUtils.mkdir(yarnConfiguration, PATH);
        log.info("Test artifacts are in " + PATH);

        Product desktopGadgetsAnalytic = new Product();
        desktopGadgetsAnalytic.setProductName("Bundle #1");
        desktopGadgetsAnalytic.setProductBundle("Bundle #1");
        desktopGadgetsAnalytic.setProductId(HashUtils.getShortHash(desktopGadgetsAnalytic.getProductName()));
        desktopGadgetsAnalytic.setProductType(ProductType.Analytic.name());
        desktopGadgetsAnalytic.setProductStatus(ProductStatus.Active.name());

        Product soapSpendingCategory = new Product();
        soapSpendingCategory.setProductName("Detergent category #1");
        soapSpendingCategory.setProductCategory("Detergent category #1");
        soapSpendingCategory.setProductId(HashUtils.getShortHash(soapSpendingCategory.getProductName()));
        soapSpendingCategory.setProductType(ProductType.Spending.name());
        soapSpendingCategory.setProductStatus(ProductStatus.Active.name());

        Product dishwasherSpendingCategory = new Product();
        dishwasherSpendingCategory.setProductName("Appliance category #2");
        dishwasherSpendingCategory.setProductFamily("Appliance family #2");
        dishwasherSpendingCategory.setProductCategory("Appliance category #2");
        dishwasherSpendingCategory.setProductId(HashUtils.getShortHash(dishwasherSpendingCategory.getProductName()));
        dishwasherSpendingCategory.setProductType(ProductType.Spending.name());
        dishwasherSpendingCategory.setProductStatus(ProductStatus.Active.name());

        Product dishwasherSpendingFamily = new Product();
        dishwasherSpendingFamily.setProductName("Appliance family #2");
        dishwasherSpendingFamily.setProductFamily("Appliance family #2");
        dishwasherSpendingFamily.setProductCategory("Appliance category #2");
        dishwasherSpendingFamily.setProductId(HashUtils.getShortHash(dishwasherSpendingFamily.getProductName()));
        dishwasherSpendingFamily.setProductCategoryId(dishwasherSpendingCategory.getProductId());
        dishwasherSpendingFamily.setProductType(ProductType.Spending.name());
        dishwasherSpendingFamily.setProductStatus(ProductStatus.Active.name());

        Product desktopGadgets = new Product();
        desktopGadgets.setProductId("bundle1");
        desktopGadgets.setProductName("Desktop gadgets bundle #1");
        desktopGadgets.setProductDescription("A bunch of desktop gadgets.");
        desktopGadgets.setProductBundle("Bundle #1");
        desktopGadgets.setProductBundleId(desktopGadgetsAnalytic.getProductId());
        desktopGadgets.setProductType(ProductType.Bundle.name());
        desktopGadgets.setProductStatus(ProductStatus.Active.name());

        Product soap = new Product();
        soap.setProductId("sku1");
        soap.setProductName("Soap");
        soap.setProductDescription("Soap with heavy foams.");
        soap.setProductCategory("Detergent category #1");
        soap.setProductCategoryId(soapSpendingCategory.getProductId());
        soap.setProductType(ProductType.Hierarchy.name());
        soap.setProductStatus(ProductStatus.Active.name());

        Product dishwasher = new Product();
        dishwasher.setProductId("sku2");
        dishwasher.setProductName("Dishwasher");
        dishwasher.setProductDescription("Heavy-duty dishwasher.");
        dishwasher.setProductLine("Appliance Line #2");
        dishwasher.setProductFamily("Appliance family #2");
        dishwasher.setProductCategory("Appliance category #2");
        dishwasher.setProductFamilyId(dishwasherSpendingFamily.getProductId());
        dishwasher.setProductCategoryId(dishwasherSpendingCategory.getProductId());
        dishwasher.setProductType(ProductType.Hierarchy.name());
        dishwasher.setProductStatus(ProductStatus.Active.name());

        productList = Arrays.asList(desktopGadgetsAnalytic, soapSpendingCategory, dishwasherSpendingCategory,
                dishwasherSpendingFamily, desktopGadgets, soap, dishwasher);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, PATH);
        log.info(String.format("Test artifacts in %s are removed.", PATH));
    }

    @Test(groups = "functional")
    public void testGetCompositeId() {
        String[] productIds = new String[] { "ABC1559C35BE7F1F45DD93669E6B252E", "ABF1560C79BA5B1F45AD97381D0E987C" };
        String[] productNames = new String[] { "All Products", "All Product Bundles" };
        String productLine = "Line 1";
        String productFamily = "Family 1";
        String productCategory = "Category 1";

        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Bundle.name(), productIds[0], null,
                productNames[0], null, null, null),
                StringUtils.join(new String[] { "Bundle", productIds[0], productNames[0] }, "__"));
        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Bundle.name(), productIds[1], null,
                productNames[1], null, null, null),
                StringUtils.join(new String[] { "Bundle", productIds[1], productNames[1] }, "__"));

        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Hierarchy.name(), productIds[0], null,
                null, null, null, null),
                "Hierarchy__" + productIds[0]);

        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Analytic.name(), productIds[0], productNames[0],
                null, null, null, null),
                "Analytic__" + productIds[0]);
        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Analytic.name(), null, productNames[1],
                null, null, null, null),
                "Analytic__" + productNames[1]);

        Assert.assertEquals(ProductUtils.getCompositeId(ProductType.Spending.name(), productIds[0], productNames[0],
                null, productCategory, productFamily, productLine),
                StringUtils.join(new String[] { "Spending", productNames[0], productCategory, productFamily, productLine }, "__"));
    }

    @Test(groups = "functional")
    public void testSaveProducts() throws IOException {
        ProductUtils.saveProducts(yarnConfiguration, PATH, productList);
        Assert.assertEquals(productList.size(), 7);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, PATH));
    }

    @Test(groups="functional", dependsOnMethods = "testSaveProducts")
    public void testCountProducts() {
        long activeHierarchy = ProductUtils.countProducts(yarnConfiguration, PATH,
                Arrays.asList(ProductType.Hierarchy.name()),
                ProductStatus.Active.name());
        Assert.assertEquals(activeHierarchy, 2l);
        long hierarchy = ProductUtils.countProducts(yarnConfiguration, PATH,
                Arrays.asList(ProductType.Hierarchy.name()));
        Assert.assertEquals(hierarchy, 2l);
        long activeHierarchyAndBundle = ProductUtils.countProducts(yarnConfiguration, PATH,
                Arrays.asList(ProductType.Hierarchy.name(), ProductType.Bundle.name()), ProductStatus.Active.name());
        Assert.assertEquals(activeHierarchyAndBundle, 3l);

    }

    @Test(groups = "functional", dependsOnMethods = "testSaveProducts")
    public void testLoadProducts() {
        loadedProductList = ProductUtils.loadProducts(yarnConfiguration, PATH, null, null);
        Assert.assertEquals(loadedProductList.size(), productList.size());
        Assert.assertEquals(loadedProductList.get(0).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(loadedProductList.get(0).getProductName(), "Bundle #1");
        Assert.assertEquals(loadedProductList.get(0).getProductBundle(), "Bundle #1");
        Assert.assertEquals(loadedProductList.get(0).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(1).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(loadedProductList.get(1).getProductName(), "Detergent category #1");
        Assert.assertEquals(loadedProductList.get(1).getProductCategory(), "Detergent category #1");
        Assert.assertEquals(loadedProductList.get(1).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(2).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(loadedProductList.get(2).getProductName(), "Appliance category #2");
        Assert.assertEquals(loadedProductList.get(2).getProductCategory(), "Appliance category #2");
        Assert.assertEquals(loadedProductList.get(2).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(3).getProductType(), ProductType.Spending.name());
        Assert.assertEquals(loadedProductList.get(3).getProductName(), "Appliance family #2");
        Assert.assertEquals(loadedProductList.get(3).getProductFamily(), "Appliance family #2");
        Assert.assertEquals(loadedProductList.get(3).getProductCategory(), "Appliance category #2");
        Assert.assertEquals(loadedProductList.get(3).getProductCategoryId(), loadedProductList.get(2).getProductId());
        Assert.assertEquals(loadedProductList.get(3).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(4).getProductId(), "bundle1");
        Assert.assertEquals(loadedProductList.get(4).getProductType(), ProductType.Bundle.name());
        Assert.assertEquals(loadedProductList.get(4).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(loadedProductList.get(4).getProductBundleId(), loadedProductList.get(0).getProductId());
        Assert.assertEquals(loadedProductList.get(4).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(5).getProductId(), "sku1");
        Assert.assertEquals(loadedProductList.get(5).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(loadedProductList.get(5).getProductName(), "Soap");
        Assert.assertEquals(loadedProductList.get(5).getProductCategoryId(), loadedProductList.get(1).getProductId());
        Assert.assertEquals(loadedProductList.get(5).getProductStatus(), ProductStatus.Active.name());

        Assert.assertEquals(loadedProductList.get(6).getProductId(), "sku2");
        Assert.assertEquals(loadedProductList.get(6).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(loadedProductList.get(6).getProductName(), "Dishwasher");
        Assert.assertEquals(loadedProductList.get(6).getProductFamilyId(), loadedProductList.get(3).getProductId());
        Assert.assertEquals(loadedProductList.get(6).getProductCategoryId(), loadedProductList.get(2).getProductId());
        Assert.assertEquals(loadedProductList.get(6).getProductStatus(), ProductStatus.Active.name());
    }

    @Test(groups = "functional", dependsOnMethods = "testLoadProducts")
    public void testGetProductMap() {
        Map<String, List<Product>> productMap = ProductUtils.getProductMap(loadedProductList);
        Assert.assertEquals(productMap.size(), 7);
        productMap.forEach((k, v) -> Assert.assertEquals(v.size(), 1));
    }

    @Test(groups = "functional", dependsOnMethods = "testLoadProducts")
    public void testGetActiveProductMap() {
        Map<String, List<Product>> productMap = ProductUtils.getActiveProductMap(loadedProductList);
        Assert.assertEquals(productMap.size(), 7);
        Assert.assertEquals(productMap.get("sku1").size(), 1);
        Assert.assertEquals(productMap.get("sku1").get(0).getProductName(), "Soap");
        Assert.assertEquals(productMap.get("sku1").get(0).getProductType(), ProductType.Hierarchy.name());
        Assert.assertEquals(productMap.get("bundle1").size(), 1);
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductName(), "Desktop gadgets bundle #1");
        Assert.assertEquals(productMap.get("bundle1").get(0).getProductType(), ProductType.Bundle.name());
        Assert.assertEquals(productMap.get("sku2").size(), 1);
        Assert.assertEquals(productMap.get("sku2").get(0).getProductName(), "Dishwasher");
        Assert.assertEquals(productMap.get("sku2").get(0).getProductType(), ProductType.Hierarchy.name());

        loadedProductList.get(6).setProductStatus(ProductStatus.Obsolete.name());
        productMap = ProductUtils.getActiveProductMap(loadedProductList);
        Assert.assertEquals(productMap.size(), 6);
        Assert.assertFalse(productMap.keySet().contains("sku2"));
        loadedProductList.get(6).setProductStatus(ProductStatus.Active.name());
    }

    @Test(groups = "functional", dependsOnMethods = "testLoadProducts")
    public void testGetActiveProductMapByTypes() {
        Map<String, List<Product>> productMap = ProductUtils.getActiveProductMap(loadedProductList,
                ProductType.Analytic.name(), ProductType.Spending.name());
        Assert.assertEquals(productMap.size(), 4);
        productMap.forEach((k, v) -> Assert.assertEquals(v.size(), 1));
    }

    @Test(groups = "functional", dependsOnMethods = "testLoadProducts")
    public void testGetProductMapByCompositeId() {
        Map<String, Product> productMap = ProductUtils.getProductMapByCompositeId(loadedProductList);
        Assert.assertEquals(productMap.size(), 7);
        Assert.assertTrue(productMap.keySet().contains("Bundle__bundle1__Bundle #1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku2"));
        Assert.assertTrue(productMap.keySet().contains("Analytic__" + HashUtils.getShortHash("Bundle #1")));
        Assert.assertTrue(productMap.keySet().contains("Spending__Detergent category #1__Detergent category #1____"));
        Assert.assertTrue(productMap.keySet()
                .contains("Spending__Appliance category #2__Appliance category #2__Appliance family #2__"));
        Assert.assertTrue(productMap.keySet()
                .contains("Spending__Appliance family #2__Appliance category #2__Appliance family #2__"));

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Active.name());
        Assert.assertEquals(productMap.size(), 7);

        loadedProductList.get(1).setProductStatus(ProductStatus.Obsolete.name());

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Active.name());
        Assert.assertEquals(productMap.size(), 6);
        Assert.assertTrue(productMap.keySet().contains("Bundle__bundle1__Bundle #1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku1"));
        Assert.assertTrue(productMap.keySet().contains("Hierarchy__sku2"));
        Assert.assertTrue(productMap.keySet().contains("Analytic__" + HashUtils.getShortHash("Bundle #1")));
        Assert.assertTrue(productMap.keySet()
                .contains("Spending__Appliance category #2__Appliance category #2__Appliance family #2__"));
        Assert.assertTrue(productMap.keySet()
                .contains("Spending__Appliance family #2__Appliance category #2__Appliance family #2__"));
        Assert.assertFalse(productMap.keySet().contains("Spending__Detergent category #1"));

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Obsolete.name());
        Assert.assertEquals(productMap.size(), 1);

        productMap = ProductUtils.getProductMapByCompositeId(loadedProductList, ProductStatus.Active.name(),
                ProductStatus.Obsolete.name());
        Assert.assertEquals(productMap.size(), 7);

        loadedProductList.get(1).setProductStatus(ProductStatus.Active.name());
    }
}
