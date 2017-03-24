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
import org.apache.commons.lang.StringUtils;
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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
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

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(amsMerged, "AMSeedMerged_TestAMSeedClean", baseSourceVersion);
        uploadBaseSourceFile(alexa, "Alexa_TestAMSeedClean", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(amsMerged.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
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
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getReportConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getJunkyardConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getMarkerConfig(boolean useTez) throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
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

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");

        Object[][] expectedData = {
                // Test markLessPopularDomainsForDUNS
                { 1L, "a.com", "01", "Name1", "Country1", "LE", "N", "Y", "0", ">10,000", 100, 100, 100000000L },
                { 2L, "b.com", "01", "Name1", "Country1", "DnB", "Y", "Y", "0", ">10,000", 100, 10, 100000000L },
                { 3L, "c.com", "01", "Name1", "Country1", "LE", "N", "Y", "0", ">10,000", 100, null, 100000000L },
                { 4L, "d.com", "02", "Name2", "Country2", "LE", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 5L, "e.com", "02", "Name2", "Country2", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L },
                { 6L, "f.com", "03", "Name3", "Country3", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L },
                { 7L, "g.com", "03", "Name3", "Country3", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 8L, "h.com", null, "NameNull", "CountryNull", "LE", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 9L, "i.com", null, "NameNull", "CountryNull", "LE", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 10L, null, "04", "Name4", "Country4", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L },
                { 11L, null, "05", "Name5", "Country5", null, "N", "Y", "0", ">10,000", 100, null, 100000000L },
                // Test markOOBEntries
                // LatticeID = 12 is removed
                { 13L, null, "07", "Name7", "Country7", "DnB", "N", "Y", null, ">10,000", 100, null, 100000000L }, 
                // Test markOrphanRecordWithDomain
                { 14L, "aa.com", "11", "Name11", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 15L, "aa.com", "12", "Name12", "CountryAA", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 16L, "bb.com", "13", "Name13", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                { 17L, "bb.com", "14", "Name14", "CountryBB", "DnB", "Y", "Y", "0", ">10,000", 100, null, 100000000L },
                // LatticeID = 18, 19, 21 are removed
                { 20L, "cc.com", "17", "Name17", "CountryCC", "DnB", "Y", "Y", "0", ">10,000", 0, null, 100000000L },
                { 22L, "dd.com", "19", "Name19", "CountryDD", "DnB", "Y", "Y", "0", ">10,000", 0, null, null },
                // LatticeID = 23 and 24 will only be left one
                { 23L, "ee.com", "20", "Name20", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, null },
                { 24L, "ee.com", "21", "Name21", "CountryEE", "DnB", "Y", "Y", "0", ">10,000", 0, null, null },
                // Test markOrphanRecordsForSmallBusiness
                // LatticeID = 25, 27 are removed
                { 26L, "aaa.com", "31", "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, null, 100000000L },
                { 28L, null, "31", "Name31", "Country31", "DnB", "N", "Y", "0", ">10,000", 100, null, 100000000L },
                { 29L, "bbb.com", null, "Name31", "Country31", "DnB", "Y", "Y", "0", "1-10", 100, null, 100000000L },
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
                "SALES_VOLUME_US_DOLLARS"
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

        Assert.assertEquals(numRows, 22, "There should be 22 rows in the result.");
        Assert.assertEquals(distinctIds.size(), 22, "There should be 22 distinct lattice ids.");
        Assert.assertFalse(hasFieldMismatchInRecord, "There are incorrect results, see logs above.");

    }
}
