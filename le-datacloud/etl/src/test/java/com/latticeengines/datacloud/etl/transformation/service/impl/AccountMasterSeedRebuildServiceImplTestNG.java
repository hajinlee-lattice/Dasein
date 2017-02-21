package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
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
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class AccountMasterSeedRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AccountMasterSeedRebuildServiceImplTestNG.class);

    private static final String LATTICEID = "LatticeID";
    private static final String KEY = "Key";
    @Autowired
    PipelineSource source;

    @Autowired
    AccountMasterSeedMerged baseSource;

    @Autowired
    AlexaMostRecent baseSource2;

    @Autowired
    AccountMasterSeed accountMasterSeedSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterSeed";
    String targetVersion;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "AccountMasterIntermediateSeed_TestAccountMasterSeed",
                "2017-01-09_19-12-43_UTC");
        uploadBaseSourceFile(baseSource2,
                baseSource2.getSourceName() + "_Test" + accountMasterSeedSource.getSourceName(),
                "2017-01-09_19-12-43_UTC");
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
        return hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            baseSources.add(baseSource2.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("accountMasterSeedMarkerTransformer");
            step1.setTargetSource("AccountMasterSeedMarked");
            String confParamStr1 = getMarkerConfig();
            step1.setConfiguration(confParamStr1);
            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTargetSource(targetSourceName);
            step2.setTransformer("accountMasterSeedCleanupTransformer");

            String confParamStr2 = getCleanupConfig();

            step2.setConfiguration(confParamStr2);
            // -----------
            TransformationStepConfig step3 = new TransformationStepConfig();
            step3.setInputSteps(inputSteps);
            step3.setTargetSource("AccountMasterSeedReport");
            step3.setTransformer("accountMasterSeedReportTransformer");

            String confParamStr3 = getReportConfig();

            step3.setConfiguration(confParamStr3);
            // // -----------
            // TransformationStepConfig step4 = new TransformationStepConfig();
            // step4.setInputSteps(inputSteps);
            // step4.setTargetSource("AccountMasterSeedSecondaryDomain");
            // step4.setTransformer("accountMasterSeedSecondaryDomainTransformer");
            //
            // String confParamStr4 = getReportConfig();
            //
            // step4.setConfiguration(confParamStr4);
            // -----------
            TransformationStepConfig step5 = new TransformationStepConfig();
            step5.setInputSteps(inputSteps);
            step5.setTargetSource("AccountMasterSeedJunkyard");
            step5.setTransformer("accountMasterSeedJunkyardTransformer");

            String confParamStr5 = getJunkyardConfig();

            step5.setConfiguration(confParamStr5);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            //steps.add(step4);
            steps.add(step5);
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

    private String getMarkerConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
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
                { "DnB_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_01_COMPANY_PHONE", "DnB_01_EMPLOYEE_RANGE",
                        "DnB_01_COMPANY_DESCRIPTION", "DnB_01_ZIPCODE", 3, "DnB_01_SIC_CODE", "Y", "DnB_01_CITY",
                        "DnB_01_INDUSTRY", "DnB_01_NAME", null, "01", "DnB_01_STATE", "DnB_01_NAICS_CODE",
                        "DnB_01_REVENUE_RANGE", "DnB_01_ADDR", "USA", "c.com", "Y", "0", 7L, 1485366555040L },
                { "DnB_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_01_COMPANY_PHONE", "DnB_01_EMPLOYEE_RANGE",
                        "DnB_01_COMPANY_DESCRIPTION", "DnB_01_ZIPCODE", 3, "DnB_01_SIC_CODE", "Y", "DnB_01_CITY",
                        "DnB_01_INDUSTRY", "DnB_01_NAME", null, "01", "DnB_01_STATE", "DnB_01_NAICS_CODE",
                        "DnB_01_REVENUE_RANGE", "DnB_01_ADDR", "USA", "e.com", "N", "0", 8L, 1485366555040L },
                { null, null, null, null, null, null, 1, null, "Y", "LE_NULL_CITY_2", null, "LE_NULL_NAME_2", null,
                        null, "LE_NULL_STATE_2", null, null, null, "BRAZIL", "d.com", "Y", null, 3L, 1485366555040L },
                { null, null, null, null, null, null, 1, null, "Y", "LE_NULL_CITY_2", null, "LE_NULL_NAME_2", null,
                        null, "LE_NULL_STATE_2", null, null, null, "BRAZIL", "T_d.com", "Y", null, 11L,
                        1485366555040L },
                { "2DnB_2_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_2_01_COMPANY_PHONE",
                        "DnB_2_01_EMPLOYEE_RANGE", "DnB_2_01_COMPANY_DESCRIPTION", "DnB_2_01_ZIPCODE", 3,
                        "DnB_2_01_SIC_CODE", "Y", "DnB_2_01_CITY", "DnB_2_01_INDUSTRY", "DnB_2_01_NAME", null, "201",
                        "DnB_2_01_STATE", "DnB_2_01_NAICS_CODE", "DnB_2_01_REVENUE_RANGE", "DnB_2_01_ADDR", "USA",
                        "T_b.com", "N", "0", 13L, 1485366555040L },
                { "DnB_04_PRIMARY_DUNS", "germany", "DnB_04_COMPANY_PHONE", "DnB_04_EMPLOYEE_RANGE",
                        "DnB_04_COMPANY_DESCRIPTION", "DnB_04_ZIPCODE", 6, "DnB_04_SIC_CODE", "Y", "DnB_04_CITY",
                        "DnB_04_INDUSTRY", "DnB_04_NAME", null, "04", "DnB_04_STATE", "DnB_04_NAICS_CODE",
                        "DnB_04_REVENUE_RANGE", "DnB_04_ADDR", "GERMANY", null, "N", "0", 6L, 1485366555040L },
                { "2DnB_2_04_PRIMARY_DUNS", "germany", "DnB_2_04_COMPANY_PHONE", "DnB_2_04_EMPLOYEE_RANGE",
                        "DnB_2_04_COMPANY_DESCRIPTION", "DnB_2_04_ZIPCODE", 6, "DnB_2_04_SIC_CODE", "Y",
                        "DnB_2_04_CITY", "DnB_2_04_INDUSTRY", "DnB_2_04_NAME", null, "204", "DnB_2_04_STATE",
                        "DnB_2_04_NAICS_CODE", "DnB_2_04_REVENUE_RANGE", "DnB_2_04_ADDR", "GERMANY", null, "N", "0",
                        14L, 1485366555040L },
                { "DnB_02_PRIMARY_DUNS", "ca", "DnB_02_COMPANY_PHONE", "DnB_02_EMPLOYEE_RANGE",
                        "DnB_02_COMPANY_DESCRIPTION", "DnB_02_ZIPCODE", 4, "DnB_02_SIC_CODE", "Y", "DnB_02_CITY",
                        "DnB_02_INDUSTRY", "DnB_02_NAME", null, "02", "DnB_02_STATE", "DnB_02_NAICS_CODE",
                        "DnB_02_REVENUE_RANGE", "DnB_02_ADDR", "CANADA", "a.com", "Y", "0", 2L, 1485366555040L },
                { "DnB_03_PRIMARY_DUNS", "people's republic of china", "DnB_03_COMPANY_PHONE", "DnB_03_EMPLOYEE_RANGE",
                        "DnB_03_COMPANY_DESCRIPTION", "DnB_03_ZIPCODE", 5, "DnB_03_SIC_CODE", "Y", "DnB_03_CITY",
                        "DnB_03_INDUSTRY", "DnB_03_NAME", null, "03", "DnB_03_STATE", "DnB_03_NAICS_CODE",
                        "DnB_03_REVENUE_RANGE", "DnB_03_ADDR", "CHINA", "a.com", "Y", "0", 1L, 1485366555040L },
                { "DnB_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_01_COMPANY_PHONE", "DnB_01_EMPLOYEE_RANGE",
                        "DnB_01_COMPANY_DESCRIPTION", "DnB_01_ZIPCODE", 3, "DnB_01_SIC_CODE", "Y", "DnB_01_CITY",
                        "DnB_01_INDUSTRY", "DnB_01_NAME", null, "01", "DnB_01_STATE", "DnB_01_NAICS_CODE",
                        "DnB_01_REVENUE_RANGE", "DnB_01_ADDR", "USA", "a.com", "N", "0", 4L, 1485366555040L },
                { "2DnB_2_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_2_01_COMPANY_PHONE",
                        "DnB_2_01_EMPLOYEE_RANGE", "DnB_2_01_COMPANY_DESCRIPTION", "DnB_2_01_ZIPCODE", 3,
                        "DnB_2_01_SIC_CODE", "Y", "DnB_2_01_CITY", "DnB_2_01_INDUSTRY", "DnB_2_01_NAME", null, "201",
                        "DnB_2_01_STATE", "DnB_2_01_NAICS_CODE", "DnB_2_01_REVENUE_RANGE", "DnB_2_01_ADDR", "USA",
                        "T_a.com", "Y", "0", 12L, 1485366555040L },
                { "DnB_01_PRIMARY_DUNS", "unITED STAtes 425@%#$@", "DnB_01_COMPANY_PHONE", "DnB_01_EMPLOYEE_RANGE",
                        "DnB_01_COMPANY_DESCRIPTION", "DnB_01_ZIPCODE", 3, "DnB_01_SIC_CODE", "Y", "DnB_01_CITY",
                        "DnB_01_INDUSTRY", "DnB_01_NAME", null, "01", "DnB_01_STATE", "DnB_01_NAICS_CODE",
                        "DnB_01_REVENUE_RANGE", "DnB_01_ADDR", "USA", "b.com", "N", "0", 5L, 1485366555040L } };

        String[] fieldNames = new String[] {
                "LE_PRIMARY_DUNS", //
                "LE_COUNTRY", //
                "LE_COMPANY_PHONE", //
                "LE_EMPLOYEE_RANGE", //
                "LE_COMPANY_DESCRIPTION", //
                "ZipCode", //
                "LE_NUMBER_OF_LOCATIONS", //
                "LE_SIC_CODE", //
                "LE_IS_PRIMARY_LOCATION", //
                "City", //
                "LE_INDUSTRY", //
                "Name", //
                "GLOBAL_ULTIMATE_DUNS_NUMBER", //
                "DUNS", //
                "State", //
                "LE_NAICS_CODE", //
                "LE_REVENUE_RANGE", //
                "Street", //
                "Country", //
                "Domain", //
                "LE_IS_PRIMARY_DOMAIN", //
                "OUT_OF_BUSINESS_INDICATOR", //
                "LatticeID", //
                "LE_Last_Upload_Date" //
        };

        Map<Long, Map<String, Object>> latticeIdToData = new HashMap<>();
        for (Object[] data: expectedData) {
            Long latticeId = (Long) data[data.length - 2];
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
            distinctIds.add(latticeId);

            Map<String, Object> data = latticeIdToData.get(latticeId);

            List<String> misMatched = new ArrayList<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                Object val = record.get(field.name());
                if (val instanceof Utf8) {
                    val = val.toString();
                }
                Object expectedVal = data.get(field.name());
                if ((val == null && expectedVal != null) //
                        || (val != null && !val.equals(expectedVal))) {
                    misMatched.add(field.name() + "=[" + val + " - " + expectedVal + "]");
                    hasFieldMismatchInRecord = true;
                }
            }
            if (hasFieldMismatchInRecord) {
                log.warn(StringUtils.join(misMatched, ", "));
            }

            System.out.println(latticeId + ":" + record);
            numRows++;
        }

        Assert.assertEquals(numRows, 12, "There should be 12 rows in the result.");
        Assert.assertEquals(distinctIds.size(), 12, "There should be 12 distinct lattice ids.");
        Assert.assertFalse(hasFieldMismatchInRecord, "There are incorrect results, see logs above.");

    }
}
