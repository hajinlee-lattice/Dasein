package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

public class InputFileValidatorTestNG {
    private InputFileValidator inputFileValidator;


    @BeforeClass(groups = "unit")
    public void setup() {
        inputFileValidator = new InputFileValidator();
    }

    @Test(groups = "unit")
    public void testMergeHierarchyProducts() {
        List<Product> hierarchyProducts = Arrays.asList(
                new Product("1", null, null, null, null, "l1", "f1", "c1", null, null, null, null, null),
                new Product("2", null, null, null, null, null, "f2", "c2", null, null, null, null, null),
                new Product("3", null, null, null, null, null, null, "c3", null, null, null, null, null),
                new Product("1", null, null, null, null, "l1", "f1", "c1", null, null, null, null, null),
                new Product("4", null, null, null, null, "l1", "f2", "c2", null, null, null, null, null),
                new Product("3", null, null, null, null, null, null, "c3", null, null, null, null, null));
        List<Product> currentProducts = Collections.emptyList();
        List<String> errors = new ArrayList<>();
        List<Product> result = inputFileValidator.mergeProducts(hierarchyProducts, currentProducts, errors);
        Assert.assertEquals(result.size(), 11);
        Assert.assertEquals(errors.size(), 0);
        result.forEach(product -> {
            Assert.assertTrue(product.getProductType().equals(ProductType.Spending.name()) ||
                    product.getProductType().equals(ProductType.Hierarchy.name()));
        });
        hierarchyProducts = Arrays
                .asList(new Product("3", null, null, null, null, null, null, "c3", null, null, null, null, null),
                        new Product("3", null, null, null, null, null, null, "c4", null, null, null, null, null));
        errors = new ArrayList<>();
        inputFileValidator.mergeProducts(hierarchyProducts, currentProducts, errors);
        Assert.assertEquals(errors.size(), 1);
        errors.get(0)
                .equals("Product with same SKU [SKU=3] has different product families " + "[Family1=c3, Family2=c4].");
        hierarchyProducts = Collections.singletonList(
                new Product("1", null, null, null, null, "l1", null, "c1", null, null, null, null, null));
        errors = new ArrayList<>();
        inputFileValidator.mergeProducts(hierarchyProducts, currentProducts, errors);
        Assert.assertEquals(errors.size(), 1);
        errors.get(0).startsWith("Product hierarchy has level-3 but does not have level-2 or level-1.");
    }

}
