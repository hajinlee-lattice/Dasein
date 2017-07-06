package com.latticeengines.datacloud.etl.transformation.service.impl;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedCleanup;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedJunkyard;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedMarker;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedReport;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedCleanPipelineTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AMSeedCleanPipelineTestNG.class);

    private static final String LATTICEID = "LatticeID";

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMasterSeedMerged amsMerged;

    @Autowired
    AlexaMostRecent alexa;

    @Autowired
    AccountMasterSeed accountMasterSeedSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    String targetSourceName = "AccountMasterSeed";
    String targetVersion;

    ObjectMapper om = new ObjectMapper();

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
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(amsMerged.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
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
            step2.setTargetSource(targetSourceName);
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
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    private String getCleanupConfig() throws JsonProcessingException {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getReportConfig() throws JsonProcessingException {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getJunkyardConfig() throws JsonProcessingException {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getMarkerConfig(boolean useTez) throws JsonProcessingException {
        AMSeedMarkerConfig conf = new AMSeedMarkerConfig();
        String[] srcPriorityToMrkPriDom = { "Orb", "HG", "DnB", "RTS" };
        conf.setSrcPriorityToMrkPriDom(srcPriorityToMrkPriDom);
        ObjectNode on = om.valueToTree(conf);
        if (useTez) {
            TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
            engineConfiguration.setEngine("TEZ");
            on.set("EngineConfig", om.valueToTree(engineConfiguration));
        } else {
            TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
            engineConfiguration.setEngine("FLINK");
            on.set("EngineConfig", om.valueToTree(engineConfiguration));
        }
        return om.writeValueAsString(on);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
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

    private Object[][] amsData = new Object[][] { //
            /* Test markLessPopularDomainsForDUNS */
            { 1L, "a.com", "01", "Name1", "Country1", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, "01" }, //
            { 2L, "b.com", "01", "Name1", "Country1", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "01" }, //
            { 3L, "c.com", "01", "Name1", "Country1", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, "01" }, //
            { 4L, "d.com", "02", "Name2", "Country2", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, "02" }, //
            { 5L, "e.com", "02", "Name2", "Country2", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "02" }, //
            { 6L, "f.com", "03", "Name3", "Country3", "DnB", "N", "Y", "0", ">10,000", 100, 100000000L, "03" }, //
            { 7L, "g.com", "03", "Name3", "Country3", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "03" }, //
            { 8L, "h.com", null, "NameNull", "CountryNull", "Orb", "N", "Y", "0", ">10,000", 100, 100000000L, null }, //
            { 9L, "i.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, 100000000L, null }, //
            { 10L, null, "04", "Name4", "Country4", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "04" }, //
            { 11L, null, "05", "Name5", "Country5", null, "Y", "Y", "0", ">10,000", 100, 100000000L, "05" }, //
            // 1 duns with multiple domains - test domain source priority
            { 30L, "j.com", "06", "Name6", "Country6", "RTS", "Y", "Y", "0", ">10,000", 100, 100000000L, "06" }, //
            { 31L, "k.com", "06", "Name6", "Country6", "HG", "Y", "Y", "0", ">10,000", 100, 100000000L, "06" }, //
            { 32L, "l.com", "06", "Name6", "Country6", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "06" }, //
            { 33L, "m.com", "06", "Name6", "Country6", null, "Y", "Y", "0", ">10,000", 100, 100000000L, "06" }, //
            // If 1 duns has a domain same as some domain of corresponding duduns,
            // this domain has lower priority to be marked as primary domain
            /*
            { 34L, "n.com", "07", "Name7", "Country7", "DnB", "N", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            { 35L, "o.com", "07", "Name7", "Country7", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            { 36L, "n.com", "08", "Name8", "Country8", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            { 37L, "p.com", "08", "Name8", "Country8", "DnB", "N", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            { 38L, "n.com", "09", "Name9", "Country9", "DnB", "N", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            { 39L, "o.com", "09", "Name9", "Country9", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "07" }, //
            */
            /* Test markOOBEntries */
            // LatticeID = 12 will be removed
            { 12L, null, "46", "Name46", "Country46", "DnB", "N", "Y", "1", ">10,000", 100, 100000000L, "46" }, //
            { 13L, null, "47", "Name47", "Country47", "DnB", "N", "Y", null, ">10,000", 100, 100000000L, "47" }, //
            /* Test markOrphanRecordWithDomain */
            { 14L, "aa.com", "11", "Name11", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "11" }, //
            { 15L, "aa.com", "12", "Name12", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "12" }, //
            { 16L, "bb.com", "13", "Name13", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "13" }, //
            { 17L, "bb.com", "14", "Name14", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "14" }, //
            // LatticeID = 18, 19, 21 will be removed
            { 18L, "bb.com", "15", "Name15", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 0, 200000000L, "15" }, //
            { 19L, "cc.com", "16", "Name16", "CountryCC", "DnB", "Y", "Y", "0", ">10,000", 0, null, "16" }, //
            { 20L, "cc.com", "17", "Name17", "CountryCC", "DnB", "Y", "Y", "0", ">10,000", 0, 100000000L, "17" }, //
            { 21L, "cc.com", "18", "Name18", "CountryCC", "DnB", "Y", "Y", "0", ">10,000", 0, 1000L, "18" }, //
            { 22L, "dd.com", "19", "Name19", "CountryDD", "DnB", "Y", "Y", "0", ">10,000", 0, null, "19" }, //
            // LatticeID = 23 and 24 will only be left one
            { 23L, "ee.com", "20", "Name20", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, "20" }, //
            { 24L, "ee.com", "21", "Name21", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, "21" }, //
            /* Test markOrphanRecordsForSmallBusiness */
            // LatticeID = 25, 27 will be removed
            { 25L, null, "31", "Name31", null, "DnB", "Y", "Y", "0", "1-10", 100, 100000000L, "31" }, //
            { 26L, "aaa.com", "31", "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, 100000000L, "31" }, //
            { 27L, null, "31", "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, 100000000L, "31" }, //
            { 28L, null, "31", "Name31", "Country31", "DnB", "Y", "Y", "0", ">10,000", 100, 100000000L, "31" }, //
            { 29L, "bbb.com", null, "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, 100000000L, null }, //
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

        Object[][] expectedData = {
                { 1L, "a.com", "01", "Name1", "Country1", "Orb", "N", "Y", "0", ">10,000", 100, 100, 100000000L, "01" },
                { 2L, "b.com", "01", "Name1", "Country1", "DnB", "Y", "Y", "0", ">10,000", 100, 10, 100000000L, "01" },
                { 3L, "c.com", "01", "Name1", "Country1", "Orb", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "01" },
                { 4L, "d.com", "02", "Name2", "Country2", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "02" },
                { 5L, "e.com", "02", "Name2", "Country2", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "02" },
                { 6L, "f.com", "03", "Name3", "Country3", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "03" },
                { 7L, "g.com", "03", "Name3", "Country3", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "03" },
                { 8L, "h.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        null },
                { 9L, "i.com", null, "NameNull", "CountryNull", "Orb", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        null },
                { 10L, null, "04", "Name4", "Country4", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L, "04" },
                { 11L, null, "05", "Name5", "Country5", null, "N", "Y", "0", ">10,000", 100, null, 100000000L, "05" },
                { 30L, "j.com", "06", "Name6", "Country6", "RTS", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "06" }, //
                { 31L, "k.com", "06", "Name6", "Country6", "HG", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "06" }, //
                { 32L, "l.com", "06", "Name6", "Country6", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "06" }, //
                { 33L, "m.com", "06", "Name6", "Country6", null, "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "06" }, //
                /*
                { 34L, "n.com", "07", "Name7", "Country7", "DnB", "Y", "Y", "0", ">10,000", 100, 1000, 100000000L,
                        "07" }, //
                { 35L, "o.com", "07", "Name7", "Country7", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "07" }, //
                { 36L, "n.com", "08", "Name8", "Country8", "DnB", "N", "Y", "0", ">10,000", 100, 1000, 100000000L,
                        "07" }, //
                { 37L, "p.com", "08", "Name8", "Country8", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "07" }, //
                { 38L, "n.com", "09", "Name9", "Country9", "DnB", "Y", "Y", "0", ">10,000", 100, 1000, 100000000L,
                        "07" }, //
                { 39L, "o.com", "09", "Name9", "Country9", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "07" }, //
                */
                { 13L, null, "47", "Name47", "Country47", "DnB", "N", "Y", null, ">10,000", 100, null, 100000000L,
                        "47" },
                { 14L, "aa.com", "11", "Name11", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "11" },
                { 15L, "aa.com", "12", "Name12", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "12" },
                { 16L, "bb.com", "13", "Name13", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "13" },
                { 17L, "bb.com", "14", "Name14", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L,
                        "14" },
                { 20L, "cc.com", "17", "Name17", "CountryCC", "DnB", "Y", "Y", "0", ">10,000", 0, null, 100000000L,
                        "17" },
                { 22L, "dd.com", "19", "Name19", "CountryDD", "DnB", "Y", "Y", "0", ">10,000", 0, null, null, "19" },
                { 23L, "ee.com", "20", "Name20", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, null, "20" },
                { 24L, "ee.com", "21", "Name21", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, null, "21" },
                { 26L, "aaa.com", "31", "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, null, 100000000L,
                        "31" },
                { 28L, null, "31", "Name31", "Country31", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L,
                        "31" },
                { 29L, "bbb.com", null, "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, null, 100000000L,
                        null },
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
            Long latticeId = (Long) record.get(LATTICEID);
            log.info(latticeId);
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
                log.warn(StringUtils.join(misMatched, ", "));
            }

            System.out.println(latticeId + ":" + record);
            numRows++;
        }

        Assert.assertEquals(numRows, 26, "There should be 26 rows in the result.");
        Assert.assertEquals(distinctIds.size(), 26, "There should be 26 distinct lattice ids.");
        Assert.assertFalse(hasFieldMismatchInRecord, "There are incorrect results, see logs above.");

    }
}
