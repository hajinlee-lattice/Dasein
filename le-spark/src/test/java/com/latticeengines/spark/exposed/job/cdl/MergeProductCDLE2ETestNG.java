package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductReport;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeProductCDLE2ETestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadTestData();

        MergeProductConfig config = new MergeProductConfig();
        SparkJobResult result = runSparkJob(MergeProduct.class, config);

        verifyResult(result);
    }

    private void uploadTestData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
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

        Object[][] upload1 = new Object[][] { //
                // VDB
                { "C6502CEEF324F4AEE00A16026A77A922", "CMT4: Bags", "CMT4: Bags", null, null, null, null, null, null, null, null, null, null },
                { "2F514266848312E14A91542F8E8A1443", "CMT3: Other Chemicals", "CMT3: Other Chemicals", null, null, null, null, null, null, null, null, null, null },
                { "5347AEB35FB506D3EBD6DDBA89A23843", "CMT4: Chromatography Supplies", "CMT4: Chromatography Supplies", null, null, null, null, null, null, null, null, null, null },
                { "650050C066EF46905EC469E9CC2921E0", "CMT2: Services", "CMT2: Services", null, null, null, null, null, null, null, null, null, null },
                { "4789E27DB75AC66B4296BD122BFC7E41", "CMT4: Reagents and Supplements for Cell Culture", "CMT4: Reagents and Supplements for Cell Culture", null, null, null, null, null, null, null, null, null, null },

                { "37FAC983470909593C234DAA05BFA398", "CMT5: Flat Sided Cell Culture Flasks and Accessories", "CMT5: Flat Sided Cell Culture Flasks and Accessories", null, null, null, null, null, null, null, null, null, null },
                { "39BA20E7FB6184F6B22C3668BCDC3C9D", "CMT2: Equipment and Instruments", "CMT2: Equipment and Instruments", null, null, null, null, null, null, null, null, null, null },
                { "2BCB24D3A0DBDA72DC143F1621108874", "CMT4: Electrodes", "CMT4: Electrodes", null, null, null, null, null, null, null, null, null, null },
                { "1DEA0FD08988E18890D12246E74576AA", "CMT3: NA-NOT MAPPED TO CMT", "CMT3: NA-NOT MAPPED TO CMT", null, null, null, null, null, null, null, null, null, null },
                { "DF5CE9F517D290D53954C04615C026EA", "CMT4: Plastic Centrifuge Tubes", "CMT4: Plastic Centrifuge Tubes", null, null, null, null, null, null, null, null, null, null },

                { "429B95ABF91C3691B8435764243A5AD5", "CMT5: Additional Cell Culture Media", "CMT5: Additional Cell Culture Media", null, null, null, null, null, null, null, null, null, null },
                { "5663805D24A24EE9E561C740854A6090", "WebDataCMT2: NA-NOT MAPPED TO CMT", "WebDataCMT2: NA-NOT MAPPED TO CMT", null, null, null, null, null, null, null, null, null, null },
                { "89EFB682650776B3A4924DC763948A26", "CMT4: Buffer(s)", "CMT4: Buffer(s)", null, null, null, null, null, null, null, null, null, null },
                { "33AF669F43DC2F1589F061A2F7900DC0", "CMT4: Isopropanol [IPA]", "CMT4: Isopropanol [IPA]", null, null, null, null, null, null, null, null, null, null },
                { "C1CE49A2FD3BB03CA9F44C4F9D7E4A2B", "CMT3: Plastic Dishes", "CMT3: Plastic Dishes", null, null, null, null, null, null, null, null, null, null },

                { "43356888902E7390269D7AF4DCD94E6F", "CMT4: Shaker Accessories", "CMT4: Shaker Accessories", null, null, null, null, null, null, null, null, null, null },
                { "AA2AE7655F28BFE08EC4D4CB387DB488", "CMT4: Acetonitrile", "CMT4: Acetonitrile", null, null, null, null, null, null, null, null, null, null },
                { "A48F113437D354134E584D8886116989", "CMT3: Proteomics", "CMT3: Proteomics", null, null, null, null, null, null, null, null, null, null },
                { "7C9A0297E56CB4CAD914B1C8357E6376", "CMT3: Controlled Environments", "CMT3: Controlled Environments", null, null, null, null, null, null, null, null, null, null },
                { "6A9B7E20FE20C3ECC34CFBD01AF35B6C", "CMT5: Filtered Pipette Specific Tips", "CMT5: Filtered Pipette Specific Tips", null, null, null, null, null, null, null, null, null, null },

                // Bundle
                { "21807495", "CMT4: Autosampler Vials and Closures", "some product description", null,  "CMT4: Autosampler Vials and Closures", null, null, null, null, null, null, null, null },
                { "25459994", "CMT3: Other Plasticware", null, null, "CMT3: Other Plasticware", null, null, null, null, null, null, null, null },
                { "7269258", "CMT3: Diagnostics", "diagnostic bundles", null, "CMT3: Diagnostics", null, null, null, null, null, null, null, null },
                { "91224090", "CMT4: Benchtop Centrifugation", "CMT4: Benchtop Centrifugation", null, "CMT4: Benchtop Centrifugation", null, null, null, null, null, null, null, null },
                { "84454578", "CMT3: Controlled Environments", "CMT3: Controlled Environments", null, "CMT3: Controlled Environments", null, null, null, null, null, null, null, null },
                { "94573850", "CMT3: Clinical Supplies", "for clinical usage", null,  "CMT3: Clinical Supplies", null, null, null, null, null, null, null, null },
                { "39529465", "CMT5: Filtered Pipette Specific Tips", null, null, "CMT5: Filtered Pipette Specific Tips", null, null, null, null, null, null, null, null },
                { "87190632", "Glassware 1", "Glassware 1", null, "CMT3: Glassware", null, null, null, null, null, null, null, null },
                { "25162382", "Glassware 2", "Glassware 2", null, "CMT3: Glassware", null, null, null, null, null, null, null, null },
                { "94414582", "CMT3: Facility Safety and Maintenance", null, null, "CMT3: Facility Safety and Maintenance", null, null, null, null, null, null, null, null },
                { "29955173", "Accessory 1", "Accessory 1", null,  "CMT4: Shaker Accessories", null, null, null, null, null, null, null, null },
                { "375698", "CMT4: Shaker Accessories", "Accessory 2", null, "CMT4: Shaker Accessories", null, null, null, null, null, null, null, null },
                { "83882166", "Shaker Accessories", "Accessory 3", null, "CMT4: Shaker Accessories", null, null, null, null, null, null, null, null },
                { "38032937", "CMT4: Solution[s] for Chemical Testing", null, null, "CMT4: Solution[s] for Chemical Testing", null, null, null, null, null, null, null, null },
                { "99811243", "Apparatus 1", "Apparatus 1", null, "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "69890948", "Apparatus 2", null, null,  "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "23001922", "Apparatus 3", null, null, "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "83226399", "Apparatus 4", "Apparatus 4", null, "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "87386411", "CMT4: Round Plastic Narrow Mouth Bottles", "CMT4: Round Plastic Narrow Mouth Bottles", null, "CMT4: Round Plastic Narrow Mouth Bottles", null, null, null, null, null, null, null, null },
                { "13347029", "CMT4: Acetonitrile", null, null, "CMT4: Acetonitrile", null, null, null, null, null, null, null, null },
                { "94573850", "CMT3: Clinical Supplies", "for clinical usage", null,  "CMT3: Clinical Supplies", null, null, null, null, null, null, null, null },
                { "25459994", "CMT3: Other Plasticware", null, null, "CMT3: Other Plasticware", null, null, null, null, null, null, null, null },
                { "25162382", "Glassware 2", "Glassware 2", null, "CMT3: Glassware", null, null, null, null, null, null, null, null },
                { "91224090", "CMT3: Facility Safety and Maintenance", null, null, "CMT4: Round Plastic Narrow Mouth Bottles", null, null, null, null, null, null, null, null },
                { "39529465", "CCMT4: Solution[s] for Chemical Testing", null, null, "CMT4: Solution[s] for Chemical Testing", null, null, null, null, null, null, null, null },
                { "21807495", "CMT4: Autosampler Vials and Closures", "should trigger a warning message", null,  "CMT4: Autosampler Vials and Closures", null, null, null, null, null, null, null, null },
                { "99811243", "Apparatus 1", "should trigger a warning message", null, "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "23001922", "Apparatus 3", "should trigger a warning message", null, "CMT4: Microbiology Apparatus", null, null, null, null, null, null, null, null },
                { "87386411", "CMT4: Round Plastic Narrow Mouth Bottles", "should trigger a warning message", null, "CMT4: Round Plastic Narrow Mouth Bottles", null, null, null, null, null, null, null, null },
                { "83882166", "Shaker Accessories", "should trigger a warning message", null, "CMT4: Shaker Accessories", null, null, null, null, null, null, null, null },

                // Hierarchy
                { "06BA560C2B089914466D58AEF8111B90", null, "Music player designed by Apple", null, null, "iPod", "Music Player", "Digital Device", null, null, null, null, null },
                { "709D28D050BF7F85AF32F20B7DB50689", null, "Music player designed by Sony", null, null, "Walkman", "Music Player", "Digital Device", null, null, null, null, null },
                { "E2B9AAB06C66ED49E15819D1E0A1481C", null, "Generic music player", null, null, "Generic Player", "Music Player", "Digital Device", null, null, null, null, null },
                { "C8E628685AEB099FE881345CA5C059A0", null, "Laptop designed by Apple", null, null, "MacBook Pro", "Laptop", "Digital Device", null, null, null, null, null },
                { "1B07887F9DB9CF5C0B7EA99676D54AC7", null, "Decent laptop designed by Lenovo", null, null, "Thinkpad T Series", "Laptop", "Digital Device", null, null, null, null, null },

                { "77EFCD9F2F92D79EA8CB031B49CA14A8", null, null, null, null, null, "Jetta", "Sedan", null, null, null, null, null },
                { "1D940F3A1AC1CFE3A9050F1308480DCA", null, "Audi A8", null, null, "A8", "Audi", "Sedan", null, null, null, null, null },
                { "82A04705D3EC7840AD6D22F9DAC4FE4B", null, "Audi A6", null, null, "A6", "Audi", "Sedan", null, null, null, null, null },
                { "6ACEEEF1D1B2CBE32EB3DFA3A35496F0", null, "All BMW sedans", null, null, null, "BMW", "Sedan", null, null, null, null, null },
                { "18BCFE1931EB75CCFDCB15B5E94A0677", null, null, null, null, "7 Series", "BMW", "Sedan", null, null, null, null, null },

                { "F08A780BE6B0421CEED41799E738C2CC", null, null, null, null, null, null, "Sedan", null, null, null, null, null },
                { "C7E372A5F3F8D7A0A5BC0DE2E5B487C9", null, "Cars in SUV category", null, null, null, null, "SUV", null, null, null, null, null },
                { "14C1081C46DD1E89E315BD3A2D478E39", null, null, null, null, null, "Television", "Entertainment", null, null, null, null, null },
                { "CB9502C9DEF618E1B29E759A37A6B0FA", null, null, null, null, null, "Game Console", "Entertainment", null, null, null, null, null },
                { "2D28CC886A665804E0608EE415FC8762", null, null, null, null, "LEGO Toy", "Toy", "Entertainment", null, null, null, null, null },

                { "1F6A6FD399D8335CFAC4E39408AF55DB", null, "Popular HB pencils", null, null, "Pencil", "Writing Tools", "Stationary", null, null, null, null, null },
                { "3EBAB6CC6D5F29C2B4648DA8AD32A75C", null, null, null, null, "Pen", "Writing Tools", "Stationary", null, null, null, null, null },
                { "416A6D7D9B4F8D19C28BA60AD96341E6", null, null, null, null, "Eraser", "Writing Tools", "Stationary", null, null, null, null, null },
                { "419A564C5F8E50952F90027CE8F38ECE", null, "All staples", null, null, null, "Staple", "Stationary", null, null, null, null, null },
                { "7369473FF057EE91F550BACFA62F9548", null, "General stationaries", null, null, null, null, "Stationary", null, null, null, null, null },

                { "E2B9AAB06C66ED49E15819D1E0A1481C", null, "Generic music player", null, null, "Generic Player", "Music Player", "Digital Device", null, null, null, null, null },
                { "C8E628685AEB099FE881345CA5C059A0", null, "Laptop designed by Apple", null, null, "MacBook Pro", "Laptop", "Digital Device", null, null, null, null, null },
                { "1F6A6FD399D8335CFAC4E39408AF55DB", null, "Popular HB pencils", null, null, "Pencil", "Writing Tools", "Stationary", null, null, null, null, null },
                { "3EBAB6CC6D5F29C2B4648DA8AD32A75C", null, null, null, null, "Pen", "Writing Tools", "Stationary", null, null, null, null, null },
                { "416A6D7D9B4F8D19C28BA60AD96341E6", null, null, null, null, "Eraser", "Writing Tools", "Stationary", null, null, null, null, null },
        };
        uploadHdfsDataUnit(upload1, fields);
    }

    @Override
    protected void verifyOutput(String output) {
        MergeProductReport report = JsonUtils.deserialize(output, MergeProductReport.class);
        System.out.println(JsonUtils.serialize(report));
//        Assert.assertEquals(report.getRecords(), 75);
//        Assert.assertEquals(report.getInvalidRecords(), 13);
//        Assert.assertEquals(report.getBundleProducts(), 42);
//        Assert.assertEquals(report.getHierarchyProducts(), 20);
//        Assert.assertEquals(report.getAnalyticProducts(), 30);
//        Assert.assertEquals(report.getSpendingProducts(), 27);
//        Assert.assertTrue(CollectionUtils.isNotEmpty(report.getErrors()));
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
//            System.out.println(record);
        });
        return true;
    }

}
