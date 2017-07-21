package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ManualSeedMapAmSeedFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ManualSeedTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedManualFixTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AMSeedManualFixTestNG.class);

    GeneralSource source = new GeneralSource("ManualSeedMergedData");

    GeneralSource baseSource1 = new GeneralSource("ManualSeed");
    GeneralSource baseSource2 = new GeneralSource("AmSeed");

    ObjectMapper om = new ObjectMapper();

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
            baseSourceStep1.add(baseSource1.getSourceName());
            baseSourceStep1.add(baseSource2.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(ManualSeedMapAmSeedFlow.TRANSFORMER_NAME);
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
        conf.setManualSeedSalesVolume("MAN_SEED_SALES");
        conf.setManualSeedTotalEmp("MAN_SEED_EMPLOYEES");
        conf.setManualSeedDuns("ManualSeedDuns");
        conf.setAmSeedDuDuns("LE_PRIMARY_DUNS");
        conf.setAmSeedDuns("DUNS");
        conf.setAmSeedDomain("Domain");
        conf.setAmSeedSalesVolume("SALES_VOLUME_US_DOLLARS");
        conf.setAmSeedTotalEmp("EMPLOYEES_HERE");
        conf.setAmSeedLeIsPrimDom("LE_IS_PRIMARY_DOMAIN");
        conf.setIsPrimaryAccount("IsPrimaryAccount");
        return om.writeValueAsString(conf);
    }

    private void prepareManualSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ManSeed_Id", String.class));
        columns.add(Pair.of("Fix_DU_Tree_Flag", String.class));
        columns.add(Pair.of("ManualSeedDuns", String.class));
        columns.add(Pair.of("ManualSeedDomain", String.class));
        columns.add(Pair.of("MAN_SEED_SALES", Long.class));
        columns.add(Pair.of("MAN_SEED_EMPLOYEES", Integer.class));
        Object[][] data = new Object[][] {
                { "1", "Y", "DUNS56", "netapp.com", 18160000000L, 90872 },
                { "2", "Y", "DUNS44", "commbank.com.au", 3930000000L, null },
                { "3", "Y", "DUNS34", "adobe.com", 21040000000L, 5403 },
                { "4", "Y", "DUNS67", "cjsc.com", 35970000000L, 10001 },
                { "5", "Y", "DUNS88", "baidu.com", 5420000000L, 45763 },
                { "6", "Y", "DUNS13", "netflix.com", 8820000000L, null },
                { "7", "Y", "DUNS23", "cisco.com", 8790000000L, 27384 },
                { "8", "Y", "DUNS77", "lattice-engines.com", 4652222222L, 82938 },
                { "9", "Y", "DUNS60", "curejoy.com", 1211222222L, 24222 },
                { "10", "Y", "DUNS18", "salesforce.com", 3172676327L, 32324 } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareAmSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Id", String.class));
        columns.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("EMPLOYEES_HERE", Integer.class));
        columns.add(Pair.of("LE_IS_PRIMARY_DOMAIN", String.class));
        columns.add(Pair.of("City", String.class));
        Object[][] data = new Object[][] {
                { "1", "DUNS14", "netapp.com", "DUNS56", 1816003450L, 8000, "Y", "Foster City" },
                { "2", "DUNS14", "merc.com", "DUNS14", 9012819290L, 9099, "Y", "Santa Clara" },
                { "3", "DUNS11", "citrix.com", "DUNS36", 3930031100L, 6031, "Y", "Sunnyvale" },
                { "4", "DUNS11", "daikin.com", "DUNS44", 21041100000L, 9031, "Y", "San Jose" },
                { "5", "DUNS99", "csx.com", "DUNS23", 35970088880L, 19843, "Y", "Newark" },
                { "6", "DUNS99", "intuit.com", "DUNS23", 3555555880L, 77773, "N", "Gilroy" },
                { "7", "DUNS90", "datos.com", "DUNS13", 35970066680L, 10893, "Y", "Palo Alto" },
                { "8", "DUNS90", "teradata.com", "DUNS90", 35555556680L, 89898, "Y", "Los Gatos" },
                { "9", null, "dsv.com", "DUNS88", 54208787870L, 44444, "Y", "Foster City" },
                { "10", "DUNS91", "lenevo.com", "DUNS67", 59090901170L, 82948, "Y", "Burlingame" },
                { "11", null, "dell.com", "DUNS34", 59444444470L, 13049, "Y", "Fremont" },
                { "12", "DUNS11", "microsoft.com", "DUNS11", 88887203333L, 8000, "Y", "Foster City" },
                { "13", "DUNS99", "google.com", "DUNS99", 12312312313L, 76789, "Y", "Menlo Park" },
                { "14", "DUNS91", "linkedin.com", "DUNS91", 19797797973L, 23490, "Y", "Redwood City" },
                { "15", null, null, "DUNS87", 1922222223L, 11110, "Y", "San Carlos" },
                { "16", "DUNS79", null, "DUNS77", 19788888873L, 22222, "Y", "Sausilato" },
                { "17", "DUNS79", "salesforce.com", null, 19000000003L, 57575, "Y", "Sacramento" },
                { "18", "DUNS14", "intel.com", "DUNS56", 13232323323L, 50900, "Y", "Union City" }, };
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Object[][] expectedDataValues = new Object[][] {
                { 82938, null, "DUNS77", 4652222222L, "16", "Y", "DUNS79", "Sausilato", "N" },
                { 13049, "adobe.com", "DUNS34", 59444444470L, "11", "Y", null, "Fremont", "Y" },
                { 27384, "google.com", "DUNS99", 8790000000L, "13", "Y", "DUNS99", "Menlo Park", "N" },
                { 27384, "intuit.com", "DUNS23", 8790000000L, "6", "N", "DUNS99", "Gilroy", "N" },
                { 90872, "netapp.com", "DUNS56", 18160000000L, "1", "Y", "DUNS14", "Foster City", "Y" },
                { 9031, "daikin.com", "DUNS44", 3930000000L, "4", "Y", "DUNS11", "San Jose", "N" },
                { 10893, "datos.com", "DUNS13", 8820000000L, "7", "Y", "DUNS90", "Palo Alto", "N" },
                { 90872, "merc.com", "DUNS14", 18160000000L, "2", "Y", "DUNS14", "Santa Clara", "N" },
                { 11110, null, "DUNS87", 19222222233L, "15", "Y", null, "San Carlos", "N" },
                { 6031, "citrix.com", "DUNS36", 3930000000L, "3", "Y", "DUNS11", "Sunnyvale", "N" },
                { 9031, "commbank.com.au", "DUNS44", 3930000000L, "4", "Y", "DUNS11", "San Jose", "Y" },
                { 8000, "microsoft.com", "DUNS11", 3930000000L, "12", "Y", "DUNS11", "Foster City", "N" },
                { 90872, "intel.com", "DUNS56", 18160000000L, "18", "Y", "DUNS14", "Union City", "N" },
                { 10001, "lenevo.com", "DUNS67", 35970000000L, "10", "Y", "DUNS91", "Burlingame", "N" },
                { 10001, "linkedin.com", "DUNS91", 35970000000L, "14", "Y", "DUNS91", "Redwood City", "N" },
                { 10001, "cjsc.com", "DUNS67", 35970000000L, "10", "Y", "DUNS91", "Burlingame", "Y" },
                { 13049, "dell.com", "DUNS34", 59444444470L, "11", "Y", null, "Fremont", "N" },
                { 44444, "dsv.com", "DUNS88", 54208787870L, "9", "Y", null, "Foster City", "N" },
                { 44444, "baidu.com", "DUNS88", 54208787870L, "9", "Y", null, "Foster City", "Y" },
                { 89898, "teradata.com", "DUNS90", 8820000000L, "8", "Y", "DUNS90", "Los Gatos", "N" },
                { 27384, "cisco.com", "DUNS23", 8790000000L, "5", "Y", "DUNS99", "Newark", "Y" },
                { 27384, "csx.com", "DUNS23", 8790000000L, "5", "Y", "DUNS99", "Newark", "N" },
                { 82938, "lattice-engines.com", "DUNS77", 4652222222L, "16", "Y", "DUNS79", "Sausilato", "Y" },
                { 10893, "netflix.com", "DUNS13", 8820000000L, "7", "Y", "DUNS90", "Palo Alto", "Y" },
                { 82938, "salesforce.com", null, 4652222222L, "17", "Y", "DUNS79", "Sacramento", "N" } };
        int rowCount = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : "+record);
            Integer numberOfEmp = null;
            if (record.get("AM_SEED_EMPLOYEES") != null) {
                numberOfEmp = (Integer) record.get("AM_SEED_EMPLOYEES");
            }
            String domain = null;
            if (record.get("AmSeedDomain") != null) {
                domain = record.get("AmSeedDomain").toString();
            }
            String duns = null;
            if (record.get("AmSeedDuns") != null) {
                duns = record.get("AmSeedDuns").toString();
            }
            Long sales = null;
            if (record.get("AM_SEED_SALES") != null) {
                sales = (Long) record.get("AM_SEED_SALES");
            }
            String isPrimaryDomain = null;
            if (record.get("LE_IS_PRIMARY_DOMAIN") != null) {
                isPrimaryDomain = record.get("LE_IS_PRIMARY_DOMAIN").toString();
            }
            String duDuns = null;
            if (record.get("AmSeedDuDuns") != null) {
                duDuns = record.get("AmSeedDuDuns").toString();
            }
            String city = null;
            if (record.get("LDC_City") != null) {
                city = record.get("LDC_City").toString();
            }
            String isPrimaryAccount = null;
            if (record.get("IsPrimaryAccount") != null) {
                isPrimaryAccount = record.get("IsPrimaryAccount").toString();
            }

            for (int i = 0; i < expectedDataValues.length; i++) {
                if (expectedDataValues[i][1] != null) {
                    if(expectedDataValues[i][1].equals(domain)) {
                        Assert.assertEquals(numberOfEmp, expectedDataValues[i][0]);
                        Assert.assertEquals(domain, expectedDataValues[i][1]);
                        Assert.assertEquals(duns, expectedDataValues[i][2]);
                        Assert.assertEquals(sales, expectedDataValues[i][3]);
                        Assert.assertEquals(isPrimaryDomain, expectedDataValues[i][5]);
                        Assert.assertEquals(duDuns, expectedDataValues[i][6]);
                        Assert.assertEquals(city, expectedDataValues[i][7]);
                        Assert.assertEquals(isPrimaryAccount, expectedDataValues[i][8]);
                    }
                }
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 25);
    }
}
