package com.latticeengines.cdl.workflow.steps;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.merge.MergeProduct;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

public class MergeProductFunctionalTestNG {
    private static final Logger logger = LoggerFactory.getLogger(MergeProductFunctionalTestNG.class);

    private MergeProduct step;
    private List<Product> currentProductList;
    private Map<String, Object> report;

    private final List<Product> productGroup1 = Arrays.asList(
            new Product("1", null, null, null,
                    "b1", null, null, null, null, null, null, null, null),
            new Product("2", null, null, null,
                    "b2", null, null, null, null, null, null, null, null));

    private final List<Product> productGroup2 = Arrays.asList(
            new Product("1", null, null, null,
                    null, "l1", "f1", "c1", null, null, null, null, null),
            new Product("2", null, null, null,
                    null, null, "f2", "c2", null, null, null, null, null),
            new Product("3", null, null, null,
                    null, null, null, "c3", null, null, null, null, null));

    private final List<Product> productGroup3 = Arrays.asList(
            new Product("1", "sku_g3p1", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("2", "sku_g3p2", null, null,
                    null, null, null, null, null, null, null, null, null),
            new Product("3", "sku_g3p3", null, null,
                    null, null, null, null, null, null, null, null, null));

    @BeforeClass(groups = "functional")
    public void setup() {
        step = new MergeProduct();
    }

    @BeforeMethod(groups = "functional")
    public void initCurrentProductList() {
        currentProductList = Collections.emptyList();
        report = new HashMap<>();
    }

    @Test(groups = "functional")
    public void testMergeVDBProducts() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup3, currentProductList, result, report);
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.get(0).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(result.get(1).getProductType(), ProductType.Analytic.name());
        Assert.assertEquals(result.get(2).getProductType(), ProductType.Analytic.name());
    }

    @Test(groups = "functional")
    public void testMergeBundleProducts() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup1, currentProductList, result, report);

        Assert.assertEquals(result.size(), 4);
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
    }

    @Test(groups = "functional")
    public void testMergeHierarchyProducts() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup2, currentProductList, result, report);
        Assert.assertEquals(result.size(), 9);
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
    }

    @Test(groups = "functional")
    public void testRepeatedMerge() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup1, currentProductList, result, report);
        List<Product> result2 = new ArrayList<>();
        step.mergeProducts(productGroup1, result, result2, report);
        Assert.assertEquals(result2.size(), 4);

        currentProductList = Collections.emptyList();
        result.clear();
        step.mergeProducts(productGroup2, currentProductList, result, report);
        result2.clear();
        step.mergeProducts(productGroup2, result, result2, report);
        Assert.assertEquals(result2.size(), 9);
    }

    @Test(groups = "functional")
    public void testObsoleteBundleMerge() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup1, currentProductList, result, report);
        List<Product> copyOfGroup = copyProductList(productGroup1);
        copyOfGroup.get(1).setProductName("sku_g1p3");
        copyOfGroup.get(1).setProductDescription("g1p3");
        copyOfGroup.get(1).setProductBundle("b3");

        List<Product> result2 = new ArrayList<>();
        step.mergeProducts(copyOfGroup, result, result2, report);
        Assert.assertEquals(result2.size(), 6);
        List<Product> b1Products = getProductByBundle(result2, "b3");
        Assert.assertNotNull(b1Products);
        Assert.assertEquals(b1Products.size(), 2);
        Assert.assertEquals(b1Products.get(0).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(b1Products.get(1).getProductStatus(), ProductStatus.Active.name());
        List<Product> b2Products = getProductByBundle(result2, "b2");
        Assert.assertNotNull(b2Products);
        Assert.assertEquals(b2Products.size(), 2);
        Assert.assertEquals(b2Products.get(0).getProductStatus(), ProductStatus.Obsolete.name());
        Assert.assertEquals(b2Products.get(1).getProductStatus(), ProductStatus.Obsolete.name());
        List<Product> b3Products = getProductByBundle(result2, "b3");
        Assert.assertNotNull(b3Products);
        Assert.assertEquals(b3Products.size(), 2);
        Assert.assertEquals(b3Products.get(0).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(b3Products.get(1).getProductStatus(), ProductStatus.Active.name());
    }

    @Test(groups = "functional")
    public void testObsoleteHierarchyMerge() {
        List<Product> result = new ArrayList<>();
        step.mergeProducts(productGroup2, currentProductList, result, report);
        List<Product> copyOfGroup = copyProductList(productGroup2);
        copyOfGroup.get(0).setProductName("sku_g2p4");
        copyOfGroup.get(0).setProductDescription("g2p4");
        copyOfGroup.get(0).setProductCategory("c4");
        List<Product> result2 = new ArrayList<>();
        step.mergeProducts(copyOfGroup, result, result2, report);
        Assert.assertEquals(result2.size(), 10);
        List<Product> c4Products = getProductByCategory(result2, "c4");
        Assert.assertNotNull(c4Products);
        Assert.assertEquals(c4Products.size(), 4);
        Assert.assertEquals(c4Products.get(0).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(c4Products.get(1).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(c4Products.get(2).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(c4Products.get(3).getProductStatus(), ProductStatus.Active.name());
        List<Product> c3Products = getProductByCategory(result2, "c3");
        Assert.assertNotNull(c3Products);
        Assert.assertEquals(c3Products.size(), 2);
        Assert.assertEquals(c3Products.get(0).getProductStatus(), ProductStatus.Active.name());
        Assert.assertEquals(c3Products.get(1).getProductStatus(), ProductStatus.Active.name());
        List<Product> c1Products = getProductByCategory(result2, "c1");
        Assert.assertNotNull(c1Products);
        Assert.assertEquals(c1Products.size(), 1);
        Assert.assertEquals(c1Products.get(0).getProductStatus(), ProductStatus.Obsolete.name());
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
