package com.latticeengines.datacloud.etl.transformation.service.impl.ams;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedJunkyard;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedCleanup;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedMarker;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedReport;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedCleanPipelineTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AMSeedCleanPipelineTestNG.class);

    private GeneralSource source = new GeneralSource("AccountMasterSeed");

    @Autowired
    private AccountMasterSeedMerged amsMerged;

    @Autowired
    private AlexaMostRecent alexa;

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAlexa();
        prepareAMSeedMerged();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("AccountMasterSeedClean");
        configuration.setVersion(targetVersion);

        // -----------
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(amsMerged.getSourceName());
        baseSources.add(alexa.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(AMSeedMarker.TRANSFORMER_NAME);
        step1.setTargetSource("AccountMasterSeedMarked");
        String confParamStr1 = getMarkerConfig(true);
        step1.setConfiguration(confParamStr1);
        // -----------
        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<Integer>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);
        step2.setTargetSource(source.getSourceName());
        step2.setTransformer(AMSeedCleanup.TRANSFORMER_NAME);

        String confParamStr2 = getCleanupConfig();

        step2.setConfiguration(confParamStr2);
        // -----------
        TransformationStepConfig step3 = new TransformationStepConfig();
        step3.setInputSteps(inputSteps);
        step3.setTargetSource("AccountMasterSeedReport");
        step3.setTransformer(AMSeedReport.TRANSFORMER_NAME);

        String confParamStr3 = getReportConfig();

        step3.setConfiguration(confParamStr3);

        // -----------
        TransformationStepConfig step4 = new TransformationStepConfig();
        step4.setInputSteps(inputSteps);
        step4.setTargetSource("AccountMasterSeedJunkyard");
        step4.setTransformer(AMSeedJunkyard.TRANSFORMER_NAME);

        String confParamStr5 = getJunkyardConfig();

        step4.setConfiguration(confParamStr5);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        // -----------
        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        return configuration;
    }

    private String getCleanupConfig() {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return JsonUtils.serialize(conf);
    }

    private String getReportConfig() {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return JsonUtils.serialize(conf);
    }

    private String getJunkyardConfig() {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return JsonUtils.serialize(conf);
    }

    private String getMarkerConfig(boolean useTez) {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        String[] srcPriorityToMrkPriDom = { "HG", "DnB", "RTS" };
        conf.setSrcPriorityToMrkPriDom(srcPriorityToMrkPriDom);
        String[] goldenDomSrcs = { "Orb" };
        conf.setGoldenDomSrcs(goldenDomSrcs);
        if (useTez) {
            return setDataFlowEngine(JsonUtils.serialize(conf), "TEZ");
        } else {
            return JsonUtils.serialize(conf);
        }
    }

    private Object[][] alexaData = new Object[][] { //
            { "a.com", 100 }, //
            { "b.com", 10 }, //
            { "n.com", 1000 }, //
    };

    private void prepareAlexa() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("URL", String.class));
        columns.add(Pair.of("Rank", Integer.class));
        uploadBaseSourceData(alexa.getSourceName(), baseSourceVersion, columns, alexaData);
    }

    // LatticeID, Domain, DUNS, Name, Country, DomainSource, LE_IS_PRIMARY_DOMAIN, LE_IS_PRIMARY_LOCATION, OUT_OF_BUSINESS_INDICATOR, 
    // LE_EMPLOYEE_RANGE, LE_NUMBER_OF_LOCATIONS, SALES_VOLUME_US_DOLLARS, LE_PRIMARY_DUNS
    private Object[][] amsData = new Object[][] { //
            /* Test markLessPopularDomainsForDUNS */
            // Compare golden domain source
            { 1L, "a.com", "DUNS01", "Name01", "Country01", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS01" }, //
            { 2L, "b.com", "DUNS01", "Name01", "Country01", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS01" }, //
            { 3L, "c.com", "DUNS01", "Name01", "Country01", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS01" }, //
            // Compare AlexaRank
            { 4L, "a.com", "DUNS02", "Name02", "Country02", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS02" }, //
            { 5L, "b.com", "DUNS02", "Name02", "Country02", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS02" }, //
            { 6L, "c.com", "DUNS02", "Name02", "Country02", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS02" }, //
            // Compare domain source priority
            { 7L, "j.com", "DUNS03", "Name03", "Country03", "RTS", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS03" }, //
            { 8L, "k.com", "DUNS03", "Name03", "Country03", "HG", "Y", "Y", "0", ">10,000", 100, 100000000L, "DUNS03" }, //
            { 9L, "l.com", "DUNS03", "Name03", "Country03", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS03" }, //
            { 10L, "m.com", "DUNS03", "Name03", "Country03", null, "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS03" }, //
            // Compare original LE_IS_PRIMARY_DOMAIN flag
            { 11L, "f.com", "DUNS04", "Name04", "Country04", null, "N", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS04" }, //
            { 12L, "g.com", "DUNS04", "Name04", "Country04", null, "Y", "Y", "0", ">10,000", 100, 100000000L,
                    "DUNS04" }, //
            // Domain-only entries
            { 13L, "h.com", null, "NameNull", "CountryNull", "Orb", "N", "Y", "0", ">10,000", 100, 100000000L, null }, //
            { 14L, "i.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, null }, //
            // DUNS-only entries
            { 15L, null, "DUNS05", "Name05", "Country05", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, "DUNS04" }, //
            { 16L, null, "DUNS06", "Name06", "Country06", null, "Y", "Y", "0", ">10,000", 100, 100000000L, "DUNS05" }, //

            /* Test markOOBEntries */
            // LatticeID = 112 will be removed
            { 112L, null, "46", "Name46", "Country46", "DnB", "N", "Y", "1", ">10,000", 100, 100000000L, "46" }, //
            { 113L, null, "47", "Name47", "Country47", "DnB", "N", "Y", null, ">10,000", 100, 100000000L, "47" }, //
    };

    private void prepareAMSeedMerged() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("DomainSource", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_DOMAIN", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_LOCATION", String.class));
        columns.add(Pair.of("OUT_OF_BUSINESS_INDICATOR", String.class));
        columns.add(Pair.of("LE_EMPLOYEE_RANGE", String.class));
        columns.add(Pair.of("LE_NUMBER_OF_LOCATIONS", Integer.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        uploadBaseSourceData(amsMerged.getSourceName(), baseSourceVersion, columns, amsData);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");

        // LatticeID, Domain, DUNS, Name, Country, DomainSource, LE_IS_PRIMARY_DOMAIN, LE_IS_PRIMARY_LOCATION, OUT_OF_BUSINESS_INDICATOR, 
        // LE_EMPLOYEE_RANGE, LE_NUMBER_OF_LOCATIONS, SALES_VOLUME_US_DOLLARS,
        // LE_PRIMARY_DUNS, LE_OperationLogs
        Object[][] expectedData = {
                /* Test markLessPopularDomainsForDUNS */
                // Compare golden domain source
                { 1L, "a.com", "DUNS01", "Name01", "Country01", "DnB", "N", "Y", "0", ">10,000", 100, 100, 100000000L,
                        "DUNS01", null },
                { 2L, "b.com", "DUNS01", "Name01", "Country01", "DnB", "N", "Y", "0", ">10,000", 100, 10, 100000000L,
                        "DUNS01", null },
                { 3L, "c.com", "DUNS01", "Name01", "Country01", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS01",
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=DomainSource=Orb]" },
                // Compare alexa rank
                { 4L, "a.com", "DUNS02", "Name02", "Country02", "DnB", "N", "Y", "0", ">10,000", 100, 100, 100000000L,
                        "DUNS02", null },
                { 5L, "b.com", "DUNS02", "Name02", "Country02", "DnB", "Y", "Y", "0", ">10,000", 100, 10, 100000000L,
                        "DUNS02",
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=Lower AlexaRank=10]" },
                { 6L, "c.com", "DUNS02", "Name02", "Country02", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS02", null },
                // Compare domain source priority
                { 7L, "j.com", "DUNS03", "Name03", "Country03", "RTS", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS03", null },
                { 8L, "k.com", "DUNS03", "Name03", "Country03", "HG", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS03",
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=DomainSource=HG]" },
                { 9L, "l.com", "DUNS03", "Name03", "Country03", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS03", null },
                { 10L, "m.com", "DUNS03", "Name03", "Country03", null, "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS03", null },
                // Compare original LE_IS_PRIMARY_DOMAIN flag
                { 11L, "f.com", "DUNS04", "Name04", "Country04", null, "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS04", null },
                { 12L, "g.com", "DUNS04", "Name04", "Country04", null, "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS04",
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=Original LE_IS_PRIMARY_DOMAIN=Y]" },
                // Domain-only entries
                { 13L, "h.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        null,
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=Domain-only Entry]" },
                { 14L, "i.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        null,
                        "[Step=AMSeedMarkerTransformer,Code=IS_PRI_DOM,Log=Domain-only Entry]" },
                // DUNS-only entries
                { 15L, null, "DUNS05", "Name05", "Country05", "Orb", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS04", null },
                { 16L, null, "DUNS06", "Name06", "Country06", null, "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "DUNS05", null },

                /* Test markOOBEntries */
                { 113L, null, "47", "Name47", "Country47", "DnB", "N", "Y", null, ">10,000", 100, null, 100000000L,
                        "47", null },

        };

        String[] fieldNames = new String[] { //
                "LatticeID", //
                "Domain", //
                "DUNS", //
                "Name", //
                "Country", //
                "DomainSource", //
                "LE_IS_PRIMARY_DOMAIN", //
                "LE_IS_PRIMARY_LOCATION", //
                "OUT_OF_BUSINESS_INDICATOR", //
                "LE_EMPLOYEE_RANGE", //
                "LE_NUMBER_OF_LOCATIONS", //
                "AlexaRank", //
                "SALES_VOLUME_US_DOLLARS", //
                "LE_PRIMARY_DUNS", //
                "LE_OperationLogs", //
        };

        Map<Long, Map<String, Object>> latticeIdToData = new HashMap<>();
        for (Object[] data: expectedData) {
            Long latticeId = (Long) data[0];
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                row.put(fieldNames[i], data[i]);
            }
            latticeIdToData.put(latticeId, row);
        }

        int numRows = 0;
        boolean hasFieldMismatchInRecord = false;
        Set<Long> distinctIds = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Long latticeId = (Long) record.get(DataCloudConstants.LATTICE_ID);
            distinctIds.add(latticeId);
            Map<String, Object> data = latticeIdToData.get(latticeId);

            List<String> misMatched = new ArrayList<>();
            for (String field : fieldNames) {
                Object val = record.get(field);
                if (val instanceof Utf8) {
                    val = val.toString();
                }
                Object expectedVal = data.get(field);
                if ((val == null && expectedVal != null) //
                        || (val != null && !val.equals(expectedVal))) {
                    misMatched.add(field + "=[" + val + " - " + expectedVal + "]");
                    hasFieldMismatchInRecord = true;
                }
            }
            if (hasFieldMismatchInRecord) {
                log.warn(String.format("Problematic record with LatticeId = %d. Mis-matched fields: %s",
                        String.valueOf(latticeId), StringUtils.join(misMatched, ", ")));
            }
            numRows++;
        }
        Assert.assertEquals(numRows, expectedData.length,
                String.format("There should be %d rows in the result.", expectedData.length));
        Assert.assertEquals(distinctIds.size(), expectedData.length,
                String.format("There should be %d distinct lattice ids.", expectedData.length));
        Assert.assertFalse(hasFieldMismatchInRecord, "There are incorrect results, see logs above.");

    }
}
