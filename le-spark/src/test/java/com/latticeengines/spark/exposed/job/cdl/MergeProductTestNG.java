package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductReport;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeProductTestNG extends SparkJobFunctionalTestNGBase {

    private List<String> unitNames;
    private List<Integer> units;

    @Test(groups = "functional")
    public void test() {
        uploadTestData();
        units = Arrays.asList(1, 0);

        MergeProductConfig config = new MergeProductConfig();
        SparkJobResult result = runSparkJob(MergeProduct.class, config);

        verifyResult(result);
    }

    private void uploadTestData() {
        unitNames = new ArrayList<>();

        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(InterfaceName.Id.name(), String.class), //
                Pair.of(InterfaceName.ProductId.name(), String.class), //
                Pair.of(InterfaceName.ProductName.name(), String.class), //
                Pair.of(InterfaceName.Description.name(), String.class), //
                Pair.of(InterfaceName.ProductType.name(), String.class), //
                Pair.of(InterfaceName.ProductBundle.name(), String.class), //
                Pair.of(InterfaceName.ProductLine.name(), String.class), //
                Pair.of(InterfaceName.ProductFamily.name(), String.class), //
                Pair.of(InterfaceName.ProductCategory.name(), String.class), //
                Pair.of(InterfaceName.ProductBundleId.name(), String.class), //
                Pair.of(InterfaceName.ProductLineId.name(), String.class), //
                Pair.of(InterfaceName.ProductFamilyId.name(), String.class), //
                Pair.of(InterfaceName.ProductCategoryId.name(), String.class), //
                Pair.of(InterfaceName.ProductStatus.name(), String.class));

        Object[][] oldProducts = new Object[][] { //
                { null, "cYvP2QxYshBPC63Q9auKTOGStkWz8bo", "CMT3: Spectroscopy", null, "Analytic", "CMT3: Spectroscopy", null, null, null, null, null, null, null, "Active" },
                { null, "4bHeUOQEuae7juu69LZLW9yM0A4gb1Bu", "CMT4: Plastic Vials", null, "Analytic", "CMT4: Plastic Vials", null, null, null, null, null, null, null, "Obsolete" },
                { null, "3dEVp6DZU1BeYalBm0f95w4rebMNrZ0H", "CMT3: Plastic Flasks", null, "Analytic", "CMT3: Plastic Flasks", null, null, null, null, null, null, null, "Active" },
                { null, "ljk8hLgseLja3VUAg6iJfXfhaHMbh5jU", "CMT4: Facility Safety", null, "Analytic", "CMT4: Facility Safety", null, null, null, null, null, null, null, "Obsolete" },
                { null, "product_1", null, null, "Bundle", "CMT3: Spectroscopy", null, null, null, "cYvP2QxYshBPC63Q9auKTOGStkWz8bo", null, null, null, "Active" },
                { null, "product_2", null, null, "Bundle", "CMT3: Spectroscopy", null, null, null, "cYvP2QxYshBPC63Q9auKTOGStkWz8bo", null, null, null, "Active" },
                { null, "product_3", null, null, "Bundle", "CMT3: Spectroscopy", null, null, null, "cYvP2QxYshBPC63Q9auKTOGStkWz8bo", null, null, null, "Obsolete" },
        };
        unitNames.add(uploadHdfsDataUnit(oldProducts, fields));

        Object[][] upload1 = new Object[][] { //
                { null, "product_3", null, "Product 3 Description", null, "CMT3: Spectroscopy", null, null, null, null, null, null, null, null },
                { null, "product_4", null, "Product 4 Description", null, "CMT3: Spectroscopy", null, null, null, null, null, null, null, null },
                { null, "product_5", null, "Product 5 Description", null, "CMT4: Plastic Vials", null, null, null, null, null, null, null, null },
                { null, "product_6", null, "Product 6 Description", null, "WebDataCMT3: Inorganic Chemicals", null, null, null, null, null, null, null, null },

                // new vdb
                { null, "1", "sku_g3p1", null, null, null, null, null, null, null, null, null, null, null },
                { null, "2", "sku_g3p2", null, null, null, null, null, null, null, null, null, null, null },
                { null, "3", "sku_g3p3", null, null, null, null, null, null, null, null, null, null, null },
                { null, "2", "sku_g3p2", null, null, null, null, null, null, null, null, null, null, null },
                { null, "3", "sku_g3p3", null, null, null, null, null, null, null, null, null, null, null },

                // new lines/families/categories
                { "product_11", null, null, "Product 11 Description", null, null, "l1", "f1", "c1", null, null, null, null, null },
                { null, "product_12", null, "Product 12 Description", null, null, "l2", "f2", "c2", null, null, null, null, null },
                { "product_13", null, null, "Product 13 Description", null, null, "l3", "f1", "c1", null, null, null, null, null },
                { null, "product_14", null, "Product 14 Description", null, null, "l4", "f2", "c2", null, null, null, null, null },
                { "product_15", null, null, "Product 15 Description", null, null, "l5", "f3", "c1", null, null, null, null, null },
                { null, "product_16", null, "Product 16 Description", null, null, "l6", "f4", "c2", null, null, null, null, null },
                { "product_17", null, null, "Product 17 Description", null, null, "l7", "f5", "c3", null, null, null, null, null },
                { null, "product_18", null, "Product 18 Description", null, null, "l8", "f6", "c4", null, null, null, null, null },
                { null, "product_19", null, "Product 19 Description", null, null, null, "f7", "c1", null, null, null, null, null },
                { null, "product_20", null, "Product 20 Description", null, null, null, null, "c5", null, null, null, null, null },

                // same sku different lines/families/categories
                { "product_31", null, null, "Product 31 Description", null, null, "l1", "f1", "c1", null, null, null, null, null },
                { "product_31", null, null, "Product 31 Description", null, null, "l3", "f1", "c1", null, null, null, null, null },
                { "product_32", null, null, "Product 32 Description", null, null, null, "f1", "c1", null, null, null, null, null },
                { "product_32", null, null, "Product 32 Description", null, null, null, "f3", "c1", null, null, null, null, null },
                { "product_33", null, null, "Product 33 Description", null, null, null, null, "c1", null, null, null, null, null },
                { "product_33", null, null, "Product 33 Description", null, null, null, null, "c2", null, null, null, null, null },

                // invalid ids
                { null, null, null, null, null, null, "l1", null, "c1", null, null, null, null, null },
                { null, null, null, null, null, null, "l2", null, "c2", null, null, null, null, null },

                // invalid vdb (no bundle, no category)
                { null, "1", null, null, null, null, "l1", "f1", null, null, null, null, null, null },
                { null, "2", null, null, null, null, "l2", null, null, null, null, null, null, null },

                // same line different family
                { "product_41", null, null, "Product 41 Description", null, null, "l9", "f1", "c1", null, null, null, null, null },
                { "product_42", null, null, "Product 42 Description", null, null, "l9", "f2", "c2", null, null, null, null, null },

                // same family different category
                { "product_43", null, null, "Product 43 Description", null, null, "l10", "f8", "c1", null, null, null, null, null },
                { "product_44", null, null, "Product 44 Description", null, null, "l11", "f8", "c2", null, null, null, null, null },
        };
        unitNames.add(uploadHdfsDataUnit(upload1, fields));
    }

    @Override
    protected List<String> getInputOrder() {
        return units.stream().map(unitNames::get).collect(Collectors.toList());
    }

    @Override
    protected void verifyOutput(String output) {
        MergeProductReport report = JsonUtils.deserialize(output, MergeProductReport.class);
        Assert.assertEquals(report.getRecords(), 33);
        Assert.assertEquals(report.getInvalidRecords(), 7);
        Assert.assertEquals(report.getBundleProducts(), 9);
        Assert.assertEquals(report.getHierarchyProducts(), 17);
        Assert.assertEquals(report.getAnalyticProducts(), 6);
        Assert.assertEquals(report.getSpendingProducts(), 26);
        Assert.assertTrue(CollectionUtils.isNotEmpty(report.getErrors()));
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
//            System.out.println(record);
        });
        return true;
    }

}
