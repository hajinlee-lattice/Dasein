package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.merge.MergeProduct;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

public class MergeProductFunctionalTestNG {

    private MergeProduct step;
    private List<Product> currentProductList;
    private Map<String, Object> report;

    private final List<Product> bundleProducts = Arrays.asList(
            new Product("1", null, null, null,
                    "b1", null, null, null, null, null, null, null, null),
            new Product("2", null, null, null,
                    "b2", null, null, null, null, null, null, null, null),
            new Product("1", null, null, null,
                    "b2", null, null, null, null, null, null, null, null),
            new Product("1", null, null, null,
                    "b1", null, null, null, null, null, null, null, null),
            new Product("1", null, "a test bundle product of id=2", null,
                    "b1", null, null, null, null, null, null, null, null));

    private final List<Product> hierarchyProducts = Arrays.asList(
            new Product("1", null, null, null,
                    null, "l1", "f1", "c1", null, null, null, null, null),
            new Product("2", null, null, null,
                    null, null, "f2", "c2", null, null, null, null, null),
            new Product("3", null, null, null,
                    null, null, null, "c3", null, null, null, null, null),
            new Product("1", null, null, null,
                    null, "l1", "f1", "c1", null, null, null, null, null),
            new Product("4", null, null, null,
                    null, "l1", "f2", "c2", null, null, null, null, null),
            new Product("3", null, null, null,
                    null, null, null, "c3", null, null, null, null, null));

    private final List<Product> vdbProducts = Arrays.asList(
            new Product("1", "sku_g3p1", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("2", "sku_g3p2", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("3", "sku_g3p3", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("2", "sku_g3p2", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("3", "sku_g3p3", null, null,
                    null, null, null, null, null, null, null, null, null));

    private final List<Product> invalidHierarchyHavingLineButNoFamily = Arrays.asList(
            new Product("1", null, null, null,
                    null, "l1", null, "c1", null, null, null, null, null),
            new Product("2", null, null, null,
                    null, "l2", null, "c2", null, null, null, null, null));

    private final List<Product> invalidHierarchySameSkusDifferentLines = Arrays.asList(
            new Product("1", null, null, null,
                    null, "l1", "f1", "c1", null, null, null, null, null),
            new Product("1", null, null, null,
                    null, "l2", "f1", "c1", null, null, null, null, null));

    private final List<Product> invalidHierarchySameSkusDifferentFamilies = Arrays.asList(
            new Product("1", null, null, null,
                    null, null, "f1", "c1", null, null, null, null, null),
            new Product("1", null, null, null,
                    null, null, "f2", "c1", null, null, null, null, null));

    private final List<Product> invalidHierarchySameSkusDifferentCategories = Arrays.asList(
            new Product("1", null, null, null,
                    null, null, null, "c1", null, null, null, null, null),
            new Product("1", null, null, null,
                    null, null, null, "c2", null, null, null, null, null));

    private final List<Product> productGroup5 = Arrays.asList(
            new Product("1", null, null, null,
                    null, "l1", "f1", null, null, null, null, null, null),
            new Product("2", null, null, null,
                    null, "l2", null, null, null, null, null, null, null));

    private final List<Product> productsWithInvalidIds = Arrays.asList(
            new Product(null, null, null, null,
                    null, "l1", null, "c1", null, null, null, null, null),
            new Product(null, null, null, null,
                    null, "l2", null, "c2", null, null, null, null, null));

    @BeforeClass(groups = "functional")
    public void setup() {
        step = new MergeProduct();
    }

    @BeforeMethod(groups = "functional")
    public void initCurrentProductList() {
        currentProductList = Collections.emptyList();

        report = step.constructMergeReport(step.countProducts(currentProductList), currentProductList.size());
        step.setMergeReport(report);
        Assert.assertEquals(report.get("Current_NumProductsInTotal"), 0);
        Assert.assertEquals(report.get("Current_NumProductIds"), 0);
        Assert.assertEquals(report.get("Current_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Current_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Current_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Current_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Current_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Current_NumObsoleteProducts"), 0);
    }

    @Test(groups = "functional")
    public void testMergeVDBProducts() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(vdbProducts, currentProductList, result, report);
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.get(0).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(result.get(1).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(result.get(2).getProductType(), ProductType.Analytic.name());

        step.updateMergeReport(vdbProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), vdbProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 3);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 0);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 3);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);
    }

    @Test(groups = "functional")
    public void testMergeBundleProducts() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(bundleProducts, currentProductList, result, report);

        Assert.assertEquals(result.size(), 5);
        result.forEach(product -> {
            Assert.assertEquals(product.getProductStatus(), ProductStatus.Active.name());
            Assert.assertTrue(product.getProductType().equals(ProductType.Analytic.name()) ||
                    product.getProductType().equals(ProductType.Bundle.name()));
        });
        Product product = getProductById(result, "1");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Bundle.name());
        Product bundleProduct = getProductById(result, product.getProductBundleId());
        Assert.assertNotNull(bundleProduct);
        Assert.assertEquals(bundleProduct.getProductType(), ProductType.Analytic.name());
        product = getProductById(result, "2");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Bundle.name());
        bundleProduct = getProductById(result, product.getProductBundleId());
        Assert.assertNotNull(bundleProduct);
        Assert.assertEquals(bundleProduct.getProductType(), ProductType.Analytic.name());

        step.updateMergeReport(this.bundleProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), this.bundleProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 5);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 2);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 2);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 2);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);
        Assert.assertTrue(report.containsKey("Merged_WarnMessage"));
        Assert.assertFalse(report.containsKey("Merged_ErrorMessage"));
    }

    @Test(groups = "functional")
    public void testMergeHierarchyProducts() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(hierarchyProducts, currentProductList, result, report);
        Assert.assertEquals(result.size(), 11);
        result.forEach(product -> {
            Assert.assertEquals(product.getProductStatus(), ProductStatus.Active.name());
            Assert.assertTrue(product.getProductType().equals(ProductType.Spending.name()) ||
                    product.getProductType().equals(ProductType.Hierarchy.name()));
        });
        Product product = getProductById(result, "1");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Hierarchy.name());
        Product spendingProduct = getProductById(result, product.getProductLineId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());
        spendingProduct = getProductById(result, product.getProductFamilyId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());
        spendingProduct = getProductById(result, product.getProductCategoryId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());

        product = getProductById(result, "2");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Hierarchy.name());
        Assert.assertNull(product.getProductLineId());
        spendingProduct = getProductById(result, product.getProductFamilyId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());
        spendingProduct = getProductById(result, product.getProductCategoryId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());

        product = getProductById(result, "3");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Hierarchy.name());
        Assert.assertNull(product.getProductLineId());
        Assert.assertNull(product.getProductFamilyId());
        spendingProduct = getProductById(result, product.getProductCategoryId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());

        product = getProductById(result, "4");
        Assert.assertNotNull(product);
        Assert.assertEquals(product.getProductType(), ProductType.Hierarchy.name());
        spendingProduct = getProductById(result, product.getProductLineId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());
        spendingProduct = getProductById(result, product.getProductFamilyId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());
        spendingProduct = getProductById(result, product.getProductCategoryId());
        Assert.assertNotNull(spendingProduct);
        Assert.assertEquals(spendingProduct.getProductType(), ProductType.Spending.name());

        step.updateMergeReport(hierarchyProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), hierarchyProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 11);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 4);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 4);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 3);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 7);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);
    }

    @Test(groups = "functional")
    public void testRepeatedMerge() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(bundleProducts, currentProductList, result, report);
        step.updateMergeReport(bundleProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), bundleProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 5);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 2);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 2);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 2);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);

        List<Product> result2 = new ArrayList<>();
        nInvalids = step.mergeProducts(bundleProducts, result, result2, report);
        Assert.assertEquals(result2.size(), 5);
        step.updateMergeReport(bundleProducts.size(), nInvalids, result2.size(), step.countProducts(result2));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), bundleProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 5);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 2);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 2);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 2);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);

        currentProductList = Collections.emptyList();
        result.clear();
        nInvalids = step.mergeProducts(hierarchyProducts, currentProductList, result, report);
        step.updateMergeReport(hierarchyProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), hierarchyProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 11);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 4);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 3);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 7);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);

        result2.clear();
        nInvalids = step.mergeProducts(hierarchyProducts, result, result2, report);
        Assert.assertEquals(result2.size(), 11);
        step.updateMergeReport(hierarchyProducts.size(), nInvalids, result2.size(), step.countProducts(result2));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), hierarchyProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 11);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 4);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 3);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 7);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);
    }

    @Test(groups = "functional")
    public void testObsoleteBundleMerge() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(bundleProducts, currentProductList, result, report);
        step.updateMergeReport(bundleProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), bundleProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 5);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 2);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 2);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 2);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);

        List<Product> copyOfGroup = copyProductList(bundleProducts);
        copyOfGroup.get(1).setProductName("sku_g1p3");
        copyOfGroup.get(1).setProductDescription("g1p3");
        copyOfGroup.get(1).setProductBundle("b3");

        List<Product> result2 = new ArrayList<>();
        nInvalids = step.mergeProducts(copyOfGroup, result, result2, report);
        Assert.assertEquals(result2.size(), 7);

        int nActive = 0;
        List<Product> b1Products = getProductByBundle(result2, "b3");
        Assert.assertNotNull(b1Products);
        Assert.assertEquals(b1Products.size(), 2);
        for (Product p : b1Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }
        }
        Assert.assertEquals(nActive, 2);

        nActive = 0;
        int nObsolete = 0;
        List<Product> b2Products = getProductByBundle(result2, "b2");
        Assert.assertNotNull(b2Products);
        Assert.assertEquals(b2Products.size(), 3);
        for (Product p : b2Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }

            if (p.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                nObsolete ++;
            }
        }
        Assert.assertEquals(nActive, 2);
        Assert.assertEquals(nObsolete, 1);

        nActive = 0;
        List<Product> b3Products = getProductByBundle(result2, "b3");
        Assert.assertNotNull(b3Products);
        Assert.assertEquals(b3Products.size(), 2);
        for (Product p : b3Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }
        }
        Assert.assertEquals(nActive, 2);

        step.updateMergeReport(bundleProducts.size(), nInvalids, result2.size(), step.countProducts(result2));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), bundleProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 7);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 2);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 3);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 0);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 0);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 3);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 0);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 1);
    }

    @Test(groups = "functional")
    public void testObsoleteHierarchyMerge() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(hierarchyProducts, currentProductList, result, report);
        step.updateMergeReport(hierarchyProducts.size(), nInvalids, result.size(), step.countProducts(result));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), hierarchyProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 11);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 4);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 4);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 3);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 7);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);

        List<Product> copyOfGroup = copyProductList(hierarchyProducts);
        copyOfGroup.get(1).setProductName("sku_g2p4");
        copyOfGroup.get(1).setProductDescription("g2p4");
        copyOfGroup.get(1).setProductCategory("c4");
        List<Product> result2 = new ArrayList<>();
        nInvalids = step.mergeProducts(copyOfGroup, result, result2, report);
        Assert.assertEquals(result2.size(), 13);

        int nActive = 0;
        int nObsolete = 0;
        List<Product> c4Products = getProductByCategory(result2, "c4");
        Assert.assertNotNull(c4Products);
        Assert.assertEquals(c4Products.size(), 3);
        for (Product p : c4Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }
            if (p.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                nObsolete ++;
            }
        }
        Assert.assertEquals(nActive, 3);
        Assert.assertEquals(nObsolete, 0);

        List<Product> c3Products = getProductByCategory(result2, "c3");
        Assert.assertNotNull(c3Products);
        Assert.assertEquals(c3Products.size(), 2);
        Assert.assertEquals(c3Products.get(0).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(c3Products.get(1).getProductStatus(), ProductStatus.Active.name());

        List<Product> c1Products = getProductByCategory(result2, "c1");
        Assert.assertNotNull(c1Products);
        Assert.assertEquals(c1Products.size(), 4);
        nActive = nObsolete = 0;
        for (Product p : c1Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }
            if (p.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                nObsolete ++;
            }
        }
        Assert.assertEquals(nActive, 4);
        Assert.assertEquals(nObsolete, 0);

        List<Product> c2Products = getProductByCategory(result2, "c2");
        Assert.assertNotNull(c2Products);
        Assert.assertEquals(c2Products.size(), 4);
        nActive = nObsolete = 0;
        for (Product p : c2Products) {
            if (p.getProductStatus().equals(ProductStatus.Active.name())) {
                nActive ++;
            }
            if (p.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                nObsolete ++;
            }
        }
        Assert.assertEquals(nActive, 4);
        Assert.assertEquals(nObsolete, 0);

        step.updateMergeReport(hierarchyProducts.size(), nInvalids, result2.size(), step.countProducts(result2));
        Assert.assertEquals(report.get("Merged_NumInputProducts"), hierarchyProducts.size());
        Assert.assertEquals(report.get("Merged_NumInvalidProducts"), 0);
        Assert.assertEquals(report.get("Merged_NumProductsInTotal"), 13);
        Assert.assertEquals(report.get("Merged_NumProductIds"), 4);
        Assert.assertEquals(report.get("Merged_NumProductBundles"), 0);
        Assert.assertEquals(report.get("Merged_NumProductHierarchies"), 4);
        Assert.assertEquals(report.get("Merged_NumProductCategories"), 4);
        Assert.assertEquals(report.get("Merged_NumProductAnalytics"), 0);
        Assert.assertEquals(report.get("Merged_NumProductSpendings"), 9);
        Assert.assertEquals(report.get("Merged_NumObsoleteProducts"), 0);
    }

    @Test(groups = "functional")
    public void testInvalidHierarchyHavingLineButNoFamily() {
        List<Product> result = new ArrayList<>();
        try {
            step.mergeProducts(invalidHierarchyHavingLineButNoFamily, currentProductList, result, report);
            Assert.fail("Should not reach.");
        } catch (Exception exc) {
            Assert.assertTrue(report.containsKey("Merged_ErrorMessage"));
        }
    }

    @Test(groups = "functional")
    public void testInvalidHierarchySameSkusDifferentCategories() {
        List<Product> result = new ArrayList<>();
        try {
            step.mergeProducts(invalidHierarchySameSkusDifferentCategories, currentProductList, result, report);
            Assert.fail("Should not reach.");
        } catch (Exception exc) {
            Assert.assertTrue(report.containsKey("Merged_ErrorMessage"));
        }
    }

    @Test(groups = "functional")
    public void testInvalidHierarchySameSkusDifferentFamilies() {
        List<Product> result = new ArrayList<>();
        try {
            step.mergeProducts(invalidHierarchySameSkusDifferentFamilies, currentProductList, result, report);
            Assert.fail("Should not reach.");
        } catch (Exception exc) {
            Assert.assertTrue(report.containsKey("Merged_ErrorMessage"));
        }
    }

    @Test(groups = "functional")
    public void testInvalidHierarchySameSkusDifferentLines() {
        List<Product> result = new ArrayList<>();
        try {
            step.mergeProducts(invalidHierarchySameSkusDifferentLines, currentProductList, result, report);
            Assert.fail("Should not reach.");
        } catch (Exception exc) {
            Assert.assertTrue(report.containsKey("Merged_ErrorMessage"));
        }
    }

    @Test(groups = "functional")
    public void testMergeInvalidVDBProducts() {
        List<Product> result = new ArrayList<>();
        try {
            step.mergeProducts(productGroup5, currentProductList, result, report);
            Assert.fail("Should not reach.");
        } catch (Exception exc) {
            Assert.assertTrue(report.containsKey("Merged_ErrorMessage"));
        }
    }

    @Test(groups = "functional")
    public void testMergeInvalidProductIds() {
        List<Product> result = new ArrayList<>();
        int nInvalids = step.mergeProducts(productsWithInvalidIds, currentProductList, result, report);
        Assert.assertEquals(nInvalids, 2);
    }

    private List<Product> getProductByBundle(List<Product> products, String bundle) {
        return products.stream()
                .filter(product -> product.getProductBundle().equals(bundle))
                .collect(Collectors.toList());
    }

    private List<Product> getProductByCategory(List<Product> products, String category) {
        return products.stream()
                .filter(product ->
                    product.getProductCategory() != null && product.getProductCategory().equals(category))
                .collect(Collectors.toList());
    }

    private Product getProductById(List<Product> products, String productId) {
        List<Product> result = products.stream()
                .filter(product -> product.getProductId().equals(productId))
                .collect(Collectors.toList());
        return result.size() > 0 ? result.get(0) : null;
    }

    private List<Product> copyProductList(List<Product> src) {
        List<Product> result = new ArrayList<>();
        src.forEach(product -> result.add(new Product(
                product.getProductId(),
                product.getProductName(),
                product.getProductDescription(),
                product.getProductType(),
                product.getProductBundle(),
                product.getProductLine(),
                product.getProductFamily(),
                product.getProductCategory(),
                product.getProductBundleId(),
                product.getProductLineId(),
                product.getProductFamilyId(),
                product.getProductCategoryId(),
                product.getProductStatus())));
        return result;
    }
}
