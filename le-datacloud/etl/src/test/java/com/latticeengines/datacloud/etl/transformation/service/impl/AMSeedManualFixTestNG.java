package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedManualFixFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedManualFixTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("ManualSeedMergedData");

    GeneralSource baseSource1 = new GeneralSource("ManualSeed");
    GeneralSource baseSource2 = new GeneralSource("AmSeed");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareManualSeedData();
        prepareAmSeedData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ManualSeedOverwrite");
            configuration.setVersion(targetVersion);

            // Initialize manualSeed Data Set and amSeed Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource2.getSourceName());
            baseSourceStep1.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(AMSeedManualFixFlow.TRANSFORMER_NAME);
            String confParamStr1 = getManualSeedDataConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getManualSeedDataConfig() throws JsonProcessingException {
        ManualSeedTransformerConfig conf = new ManualSeedTransformerConfig();
        conf.setFixTreeFlag("Fix_DU_Tree_Flag");
        conf.setManualSeedDomain("ManualSeedDomain");
        conf.setManualSeedDuns("ManualSeedDuns");
        conf.setAmSeedDuDuns("LE_PRIMARY_DUNS");
        conf.setAmSeedDuns("DUNS");
        conf.setAmSeedDomain("Domain");
        conf.setAmSeedLeIsPrimDom("LE_IS_PRIMARY_DOMAIN");
        conf.setIsPrimaryAccount("IsPrimaryAccount");
        String[][] fieldsArray = { { "MAN_SEED_EMPLOYEES", "EMPLOYEES_TOTAL" },
                { "MAN_SEED_SALES", "SALES_VOLUME_US_DOLLARS" }, { "Manual_LE_EMPLOYEE_RANGE", "LE_EMPLOYEE_RANGE" },
                { "Manual_LE_REVENUE_RANGE", "LE_REVENUE_RANGE" } };
        conf.setOverwriteFieldsArray(fieldsArray);
        return JsonUtils.serialize(conf);
    }

    private void prepareManualSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ManSeed_Id", String.class));
        columns.add(Pair.of("Fix_DU_Tree_Flag", String.class));
        columns.add(Pair.of("ManualSeedDuns", String.class));
        columns.add(Pair.of("ManualSeedDomain", String.class));
        columns.add(Pair.of("MAN_SEED_SALES", Long.class));
        columns.add(Pair.of("MAN_SEED_EMPLOYEES", Integer.class));
        columns.add(Pair.of("Manual_LE_EMPLOYEE_RANGE", String.class));
        columns.add(Pair.of("Manual_LE_REVENUE_RANGE", String.class));
        Object[][] data = new Object[][] {
                // domain+duns in manual seed is same as am seed
                { "12", "N", "DUNS63", "sbi.com", 1000000L, 2600, "2501-5000", "0-1M" },
                { "1", "Y", "DUNS56", "netapp.com", 18160000000L, 90872, ">10,000", ">10B" },
                // Fix_DU_Tree_Flag = N or empty
                { "2", "N", "DUNS47", "commbank.com.au", 3930000000L, null, null, "1-5B" },
                { "3", "", "DUNS48", "mulesoft.com", 8888880000L, 98688, ">10,000", "5B-10B" },
                // all types of keys with Fix_DU_Tree_Flag = Y
                { "5", "Y", "DUNS67", "cjsc.com", 35970000000L, 10001, ">10,000", ">10B" },
                { "6", "Y", "DUNS88", "baidu.com", 5420000000L, 45763, ">10,000", "5B-10B" },
                // testing Null value for employees field
                { "7", "Y", "DUNS13", "netflix.com", 8820000000L, null, null, "5B-10B" },
                // testing null value for sales field
                { "9", "Y", "DUNS77", "lattice-engines.com", null, 82938, ">10,000", null },
                // testing for DUNS present in manual seed but not in am seed
                { "11", "Y", "DUNS18", "salesforce.com", 3172676327L, 32324, ">10,000", "1-5B" } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareAmSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Id", String.class));
        columns.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL", Integer.class));
        columns.add(Pair.of("LE_IS_PRIMARY_DOMAIN", String.class));
        columns.add(Pair.of("LE_REVENUE_RANGE", String.class));
        columns.add(Pair.of("LE_EMPLOYEE_RANGE", String.class));
        columns.add(Pair.of("City", String.class));
        columns.add(Pair.of("DomainSource", String.class));
        Object[][] data = new Object[][] {
                // domain+duns in am seed is same as manual seed
                { "21", "DUNS63", "sbi.com", "DUNS63", 3588555440L, 53273, "Y", "1-5B", ">10,000", "Tahoe", "DnB" },
                // testing null DUNs value
                { "1", "DUNS14", "netapp.com", null, 1816003450L, 8000, "Y", "1-5B", "5001-10,000", "Foster City",
                        "Hg" },
                { "17", "DUNS79", "salesforce.com", null, 19000000003L, 57575, "Y", ">10B", ">10,000", "Sacramento",
                        "Orb" },
                // testing primary domain = N value
                { "6", "DUNS99", "intuit.com", "DUNS23", 3555555880L, 77773, "N", "1-5B", ">10,000", "Gilroy", "DnB" },
                // testing NULL DU value
                { "9", null, "dsv.com", "DUNS88", 54208787870L, 44444, "Y", ">10B", ">10,000", "Foster City", "RTS" },
                { "11", null, "dell.com", "DUNS34", 59444444470L, 13049, "Y", ">10B", ">10,000", "Fremont", "RTS" },
                // different keys with all values having primary domain = Y
                { "12", "DUNS11", "microsoft.com", "DUNS11", 88887203333L, 8000, "Y", ">10B", "5001-10,000",
                        "Foster City", "RTS" },
                { "13", "DUNS99", "google.com", "DUNS99", 12312312313L, 76789, "Y", ">10B", ">10,000", "Menlo Park",
                        "DnB" },
                { "2", "DUNS14", "merc.com", "DUNS14", 9012819290L, 9099, "Y", ">10B", ">10,000", "Santa Clara",
                        "Orb" },
                { "19", "DUNS44", "catechnologies.com", "DUNS44", 55555558873L, 12323, "Y", ">10B", ">10,000",
                        "Denali", "DnB" },
                { "20", "DUNS44", "mozilla.com", "DUNS47", 99999999009L, 22324, "Y", ">10B", ">10,000", "San Carlos",
                        "Hg" },
                // testing same DU different duns case
                { "14", "DUNS91", "linkedin.com", "DUNS91", 19797797973L, 23490, "Y", ">10B", ">10,000",
                        "Redwood City", "Orb" },
                { "10", "DUNS91", "lenevo.com", "DUNS67", 59090901170L, 82948, "Y", ">10B", ">10,000", "Burlingame",
                        "Hg" },
                { "3", "DUNS11", "citrix.com", "DUNS36", 3930031100L, 6031, "Y", "1-5B", "5001-10,000", "Sunnyvale",
                        "RTS" },
                { "4", "DUNS11", "daikin.com", "DUNS40", 21041100000L, 9031, "Y", ">10B", "5001-10,000", "San Jose",
                        "DnB" },
                { "7", "DUNS90", "datos.com", "DUNS13", 35970066680L, 10893, "Y", ">10B", ">10,000", "Palo Alto",
                        "DnB" },
                { "8", "DUNS90", "teradata.com", "DUNS90", 35555556680L, 89898, "Y", ">10B", ">10,000", "Los Gatos",
                        "Hg" },
                // testing same domains with different duns
                { "5", "DUNS99", "csx.com", "DUNS26", 35970088880L, 19843, "Y", ">10B", ">10,000", "Newark", "Orb" },
                { "18", "DUNS14", "intel.com", "DUNS56", 13232323323L, 50900, "Y", ">10B", ">10,000", "Union City",
                        "Hg" },
                // with NULL DU and domain
                { "15", null, null, "DUNS87", 1922222223L, 11110, "Y", "1-5B", ">10,000", "San Carlos", "DnB" },
                // with NULL domain
                { "16", "DUNS79", null, "DUNS77", 19788888873L, 22222, "Y", ">10B", ">10,000", "Sausilato", "DnB" }
        };
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] expectedDataValues = {
            // Id, EmployeeTotal, Domain, DUNS, salesVolume, LE_IS_PRIMARY_DOMAIN, LE_PRIMARY_DUNS, City, IsPrimaryAccount,
            // LE_EMPLOYEE_RANGE, LE_REVENUE_RANGE, DomainSource
            { 13, 76789, "google.com", "DUNS99", 12312312313L, "Y", "DUNS99", "Menlo Park", "N", ">10,000", ">10B",
                    "DnB" },
            { 6, 77773, "intuit.com", "DUNS23", 3555555880L, "N", "DUNS99", "Gilroy", "N", ">10,000", "1-5B", "DnB" },
            { 7, 10893, "datos.com", "DUNS13", 8820000000L, "Y", "DUNS90", "Palo Alto", "N", ">10,000", "5B-10B",
                    "DnB" },
            { 2, 90872, "merc.com", "DUNS14", 18160000000L, "Y", "DUNS14", "Santa Clara", "N", ">10,000", ">10B",
                    "Orb" },
            { 15, 11110, null, "DUNS87", 1922222223L, "Y", null, "San Carlos", "N", ">10,000", "1-5B", "DnB" },
            { 3, 6031, "citrix.com", "DUNS36", 3930031100L, "Y", "DUNS11", "Sunnyvale", "N", "5001-10,000", "1-5B",
                    "RTS" },
            { 12, 8000, "microsoft.com", "DUNS11", 88887203333L, "Y", "DUNS11", "Foster City", "N", "5001-10,000",
                    ">10B", "RTS" },
            { 21, 2600, "sbi.com", "DUNS63", 1000000L, "Y", "DUNS63", "Tahoe", "Y", "2501-5000", "0-1M", "Manual" },
            { 18, 90872, "intel.com", "DUNS56", 18160000000L, "Y", "DUNS14", "Union City", "N", ">10,000", ">10B",
                    "Hg" },
            { 10, 10001, "lenevo.com", "DUNS67", 35970000000L, "Y", "DUNS91", "Burlingame", "N", ">10,000", ">10B",
                    "Hg" },
            { 14, 10001, "linkedin.com", "DUNS91", 35970000000L, "Y", "DUNS91", "Redwood City", "N", ">10,000",
                    ">10B", "Orb" },
            { 21, 10001, "cjsc.com", "DUNS67", 35970000000L, "Y", "DUNS91", "Burlingame", "Y", ">10,000", ">10B",
                    "Manual" },
            { 11, 13049, "dell.com", "DUNS34", 59444444470L, "Y", null, "Fremont", "N", ">10,000", ">10B", "RTS" },
            { 9, 45763, "dsv.com", "DUNS88", 5420000000L, "Y", null, "Foster City", "N", ">10,000", "5B-10B", "RTS" },
            { 19, 45763, "baidu.com", "DUNS88", 5420000000L, "Y", null, "Foster City", "Y", ">10,000", "5B-10B",
                    "Manual" },
            { 8, 89898, "teradata.com", "DUNS90", 8820000000L, "Y", "DUNS90", "Los Gatos", "N", ">10,000", "5B-10B",
                    "Hg" },
            { 20, 19843, "csx.com", "DUNS26", 35970088880L, "Y", "DUNS99", "Newark", "N", ">10,000", ">10B", "Orb" },
            { 4, 9031, "daikin.com", "DUNS40", 21041100000L, "Y", "DUNS11", "San Jose", "N", "5001-10,000", ">10B",
                    "DnB" },
            { 16, 82938, "lattice-engines.com", "DUNS77", 19788888873L, "Y", "DUNS79", "Sausilato", "Y", ">10,000",
                    ">10B", "Manual" },
            { 7, 10893, "netflix.com", "DUNS13", 8820000000L, "Y", "DUNS90", "Palo Alto", "Y", ">10,000", "5B-10B",
                    "Manual" },
            { 17, 82938, "salesforce.com", null, 19000000003L, "Y", "DUNS79", "Sacramento", "N", ">10,000", ">10B",
                    "Orb" },
            { 24, 90872, "netapp.com", "DUNS56", 18160000000L, "Y", "DUNS14", "Union City", "Y", ">10,000", ">10B",
                    "Manual" },
            { 25, 22324, "mozilla.com", "DUNS47", 3930000000L, "Y", "DUNS44", "San Carlos", "N", ">10,000", "1-5B",
                    "Hg" },
            { 26, 22324, "commbank.com.au", "DUNS47", 3930000000L, "Y", "DUNS44", "San Carlos", "Y", ">10,000",
                    "1-5B", "Manual" },
            { 27, 12323, "catechnologies.com", "DUNS44", 55555558873L, "Y", "DUNS44", "Denali", "N", ">10,000",
                    ">10B", "DnB" } };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[2]).replace("null", "") + String.valueOf(data[3]).replace("null", ""),
                    data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String domain = "";
            if (record.get("Domain") != null) {
                domain = String.valueOf(record.get("Domain"));
            }
            String duns = "";
            if (record.get("DUNS") != null) {
                duns = String.valueOf(record.get("DUNS"));
            }
            Object[] expected = expectedData.get(domain + duns);
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_TOTAL"), expected[1]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_US_DOLLARS"), expected[4]));
            Assert.assertTrue(isObjEquals(record.get("LE_IS_PRIMARY_DOMAIN"), expected[5]));
            Assert.assertTrue(isObjEquals(record.get("LE_PRIMARY_DUNS"), expected[6]));
            Assert.assertTrue(isObjEquals(record.get("City"), expected[7]));
            Assert.assertTrue(isObjEquals(record.get("IsPrimaryAccount"), expected[8]));
            Assert.assertTrue(isObjEquals(record.get("LE_EMPLOYEE_RANGE"), expected[9]));
            Assert.assertTrue(isObjEquals(record.get("LE_REVENUE_RANGE"), expected[10]));
            Assert.assertTrue(isObjEquals(record.get("DomainSource"), expected[11]));
            rowCount++;
        }
        Assert.assertEquals(rowCount, 25);
    }
}
