package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AMStatsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class AccountMasterStatsDeploymentTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final String DATA_CLOUD_VERSION = "2.0.2";

    private static final Log log = LogFactory.getLog(AccountMasterStatsDeploymentTestNG.class);

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMaster baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterStats";
    String targetVersion = "2017-01-30_19-12-43_UTC";

    String statsJsonFileName = "AccountMasterStatsCube_Test.json";

    @Test(groups = "deployment", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, baseSource.getSourceName() + "_Test" + targetSourceName,
                "2017-01-30_19-12-43_UTC");

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

    @SuppressWarnings("deprecation")
    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        ObjectMapper om = new ObjectMapper();

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add("AccountMaster");
        step1.setBaseSources(baseSources);
        step1.setBaseVersions(null);
        step1.setTransformer("sourceDeduper");
        step1.setTargetSource("AccountMasterDeduped");
        String deduperConfig = getDeduperConfig();
        step1.setConfiguration(deduperConfig);

        ///////////////////

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps2 = new ArrayList<Integer>();
        inputSteps2.add(0);
        step2.setInputSteps(inputSteps2);
        step2.setTargetSource("accountMasterStatsMinMax");
        step2.setTransformer("accountMasterStatsMinMaxTransformer");

        AccountMasterStatisticsConfig confParam2 = getAccountMasterStatsParameters();
        String confParamStr2 = null;
        try {
            confParamStr2 = om.writeValueAsString(confParam2);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step2.setConfiguration(confParamStr2);

        /////////////////

        TransformationStepConfig step3 = new TransformationStepConfig();
        List<Integer> inputSteps3 = new ArrayList<Integer>();
        inputSteps3.add(0);
        inputSteps3.add(1);
        step3.setInputSteps(inputSteps3);
        step3.setTargetSource(targetSourceName);
        step3.setTransformer("accountMasterStatsTransformer");

        AccountMasterStatisticsConfig confParam3 = getAccountMasterStatsParameters();
        String confParamStr3 = null;
        try {
            confParamStr3 = om.writeValueAsString(confParam3);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step3.setConfiguration(confParamStr3);

        //////////////////

        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);

        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        return configuration;
    }

    private String getDeduperConfig() {
        return "{\"DedupeField\" : \"LDC_DUNS\"}";
    }

    private AccountMasterStatisticsConfig getAccountMasterStatsParameters() {
        AccountMasterStatisticsConfig param = new AccountMasterStatisticsConfig();
        Map<String, String> attributeCategoryMap = new HashMap<>();
        param.setAttributeCategoryMap(attributeCategoryMap);
        Map<String, Map<String, Long>> dimensionValuesIdMap = new HashMap<>();
        param.setDimensionValuesIdMap(dimensionValuesIdMap);
        param.setCubeColumnName("EncodedCube");
        param.setNumericalBucketsRequired(true);
        param.setDataCloudVersion(DATA_CLOUD_VERSION);

        List<String> dimensions = new ArrayList<>();
        dimensions.add("Location");
        dimensions.add("Industry");
        param.setDimensions(dimensions);
        return param;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Object[][] expectedData = getExpectedRecordList();

        String topmostCubeEncodedStr = null;
        boolean isTopLocation = false;
        boolean isTopIndustry = false;

        while (records.hasNext()) {
            GenericRecord record = records.next();

            boolean foundMatchingRecord = false;
            for (Object[] data : expectedData) {
                int idx = 0;
                boolean hasFieldMismatchInRecord = false;
                isTopLocation = false;
                isTopIndustry = false;
                String encodedCubeStr = null;

                for (Field field : record.getSchema().getFields()) {
                    if (field.name().equals("EncodedCube")) {
                        Object val = record.get(field.name());
                        if (val instanceof Utf8) {
                            val = ((Utf8) val).toString();
                        }
                        encodedCubeStr = (String) val;
                    }

                    if (!field.name().equals("EncodedCube") //
                            && !field.name().equals("PID")) {
                        Object val = record.get(field.name());
                        if (val instanceof Utf8) {
                            val = ((Utf8) val).toString();
                        }
                        Object expectedVal = data[idx];
                        if ((val == null && expectedVal != null) //
                                || (val != null && !val.equals(expectedVal))) {
                            if (val != null && val instanceof String
                                    && ((String) val).startsWith((String) expectedVal)) {
                                // consider it matching field
                            } else {
                                hasFieldMismatchInRecord = true;
                                break;
                            }
                        }

                        if (field.name().equals("Location")) {
                            Long locationId = (Long) val;
                            if (locationId.equals(1L)) {
                                isTopLocation = true;
                            }
                        }

                        if (field.name().equals("Industry")) {
                            Long industryId = (Long) val;
                            if (industryId.equals(53L)) {
                                isTopIndustry = true;
                            }
                        }

                        idx++;
                    }

                    if (field.name().equals("PID")) {
                        idx++;
                    }
                }

                if (isTopIndustry && isTopLocation //
                        && encodedCubeStr != null //
                        && topmostCubeEncodedStr == null) {
                    topmostCubeEncodedStr = encodedCubeStr;
                }

                if (!hasFieldMismatchInRecord //
                        || idx == record.getSchema().getFields().size()) {

                    foundMatchingRecord = true;
                    break;
                }
            }

            if (!foundMatchingRecord) {
                System.out.println("\n\n================" + rowNum);
                for (Field field : record.getSchema().getFields()) {
                    if (!field.name().equals("EncodedCube")) {

                        if (record.get(field.name()) == null) {
                            System.out.print(", null");
                        } else if (record.get(field.name()) instanceof Long) {
                            System.out.print(", " + record.get(field.name()) + "L");
                        } else if (record.get(field.name()) instanceof Utf8) {
                            String txt = ((Utf8) record.get(field.name())).toString();
                            txt = txt.substring(0, (txt.length() < 40 ? txt.length() : 40));
                            System.out.print(", \"" + txt + "\"");
                        } else {
                            throw new RuntimeException(record.get(field.name()).getClass().getName());
                        }
                    }
                }
                System.out.println("================\n");
            }
            Assert.assertTrue(foundMatchingRecord);

            rowNum++;
        }
        System.out.println();
        Assert.assertEquals(rowNum, 178);

        InputStream expectedCubeStream = ClassLoader.getSystemResourceAsStream("sources/" + statsJsonFileName);
        AccountMasterCube expectedCube = null;
        AccountMasterCube actualCube = null;

        ObjectMapper om = new ObjectMapper();
        try {
            expectedCube = om.readValue(expectedCubeStream, AccountMasterCube.class);

            actualCube = AMStatsUtils.decompressAndDecode(topmostCubeEncodedStr, AccountMasterCube.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        compareCubes(expectedCube, actualCube);
    }

    private void compareCubes(AccountMasterCube expectedCube, AccountMasterCube actualCube) {
        Assert.assertEquals(actualCube.getNonNullCount(), expectedCube.getNonNullCount());
        for (String attr : actualCube.getStatistics().keySet()) {
            AttributeStatsDetails actualRowBasedAttrStats = actualCube.getStatistics().get(attr)
                    .getRowBasedStatistics();
            AttributeStatsDetails expectedRowBasedAttrStats = expectedCube.getStatistics().get(attr)
                    .getRowBasedStatistics();
            if (actualRowBasedAttrStats.getBuckets() != null) {
                Assert.assertNotNull(actualRowBasedAttrStats.getBuckets().getType());
                Assert.assertEquals(actualRowBasedAttrStats.getBuckets().getType(),
                        expectedRowBasedAttrStats.getBuckets().getType());

                if (CollectionUtils.isNotEmpty(actualRowBasedAttrStats.getBuckets().getBucketList())) {
                    Assert.assertEquals(actualRowBasedAttrStats.getBuckets().getBucketList().size(),
                            expectedRowBasedAttrStats.getBuckets().getBucketList().size());

                    Long prevBucketId = null;
                    for (int i = 0; i < actualRowBasedAttrStats.getBuckets().getBucketList().size(); i++) {
                        Bucket actualBkt = actualRowBasedAttrStats.getBuckets().getBucketList().get(i);
                        Bucket expectedBkt = expectedRowBasedAttrStats.getBuckets().getBucketList().get(i);

                        compareBuckets(expectedBkt, actualBkt);

                        if (prevBucketId != null) {
                            Assert.assertTrue(prevBucketId < actualBkt.getId());
                        }
                        prevBucketId = actualBkt.getId();
                    }
                }
            }

            Assert.assertEquals(actualRowBasedAttrStats.getNonNullCount(), expectedRowBasedAttrStats.getNonNullCount());
        }
    }

    private void compareBuckets(Bucket expectedBkt, Bucket actualBkt) {
        Assert.assertEquals(actualBkt.getBucketLabel(), expectedBkt.getBucketLabel());
        Assert.assertEquals(actualBkt.getCount(), expectedBkt.getCount());
        Assert.assertEquals(actualBkt.getId(), expectedBkt.getId());

        if (expectedBkt.getEncodedCountList() != null) {
            Assert.assertEquals(actualBkt.getEncodedCountList().length, expectedBkt.getEncodedCountList().length);
            for (int i = 0; i < expectedBkt.getEncodedCountList().length; i++) {
                Assert.assertEquals(actualBkt.getEncodedCountList()[i], expectedBkt.getEncodedCountList()[i]);
            }
        }
    }

    private Object[][] getExpectedRecordList() {
        Object[][] expectedData = {
                { 1L, 326L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 343L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 402L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 421L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 502L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 608L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 660L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 763L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 824L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 1L, 1012L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 1L, 1034L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 285L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 328L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 386L, 1057L, 1069L, 1080L, 1081L, 6L, "618@6|616@6|619@6|622@6|614@6|621@6|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 423L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 481L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 608L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 2L, 715L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 2L, 763L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 2L, 955L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, DATA_CLOUD_VERSION },
                { 2L, 1048L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, DATA_CLOUD_VERSION },
                { 8L, 905L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 22L, DATA_CLOUD_VERSION },
                { 15L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 23L, DATA_CLOUD_VERSION },
                { 18L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 24L, DATA_CLOUD_VERSION },
                { 1L, 423L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 491L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 618L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 665L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 733L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 832L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 837L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 840L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 867L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 1L, 884L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 1L, 905L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 222L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 343L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 410L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 621L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 719L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 789L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 2L, 805L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 2L, 825L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 2L, 867L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, DATA_CLOUD_VERSION },
                { 2L, 888L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, DATA_CLOUD_VERSION },
                { 18L, 176L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, DATA_CLOUD_VERSION },
                { 1L, 53L, 1057L, 1069L, 1080L, 1081L, 122L, "618@122|616@122|619@122|622@122|614@122|",
                        "314@16|315@16|316@16|317@16|318@16|319@1", null, null, 1L, DATA_CLOUD_VERSION },
                { 1L, 117L, 1057L, 1069L, 1080L, 1081L, 5L, "618@5|616@5|619@5|622@5|614@5|621@5|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 151L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 222L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 248L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 285L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 480L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 730L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 825L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 2L, 660L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 2L, 665L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 822L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 824L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 840L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 7L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null, null,
                        15L, DATA_CLOUD_VERSION },
                { 10L, 1034L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 11L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 12L, 783L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 14L, 222L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 15L, 612L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, DATA_CLOUD_VERSION },
                { 1L, 115L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 190L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 226L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 232L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 257L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 541L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 613L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 742L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 761L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 1L, 789L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 1L, 820L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 1L, 1015L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 147L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 259L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 462L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 502L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 534L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 2L, 750L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 2L, 979L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 2L, 986L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, DATA_CLOUD_VERSION },
                { 2L, 1012L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, DATA_CLOUD_VERSION },
                { 10L, 556L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, DATA_CLOUD_VERSION },
                { 1L, 120L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 273L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 386L, 1057L, 1069L, 1080L, 1081L, 7L, "618@7|616@7|619@7|622@7|614@7|621@7|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 414L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 425L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 556L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 955L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 990L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 1048L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 2L, 232L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 2L, 257L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 273L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 326L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 421L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 427L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 556L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 911L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 7L, 758L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 1L, 259L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 333L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 427L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 679L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 715L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 719L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 750L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 783L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 2L, 117L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 2L, 120L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 2L, 190L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 374L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 480L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 541L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 625L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 9L, 733L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 11L, 151L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 12L, 693L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 14L, 414L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 1L, 176L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 328L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 374L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 455L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 462L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 481L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 534L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 621L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 625L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 1L, 693L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 10L, DATA_CLOUD_VERSION },
                { 1L, 703L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 1L, 758L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 1L, 805L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 1L, 822L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 1L, 979L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 333L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 402L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 2L, 455L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 2L, 613L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 2L, 679L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, DATA_CLOUD_VERSION },
                { 2L, 703L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, DATA_CLOUD_VERSION },
                { 2L, 731L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, DATA_CLOUD_VERSION },
                { 2L, 733L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 23L, DATA_CLOUD_VERSION },
                { 2L, 990L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 24L, DATA_CLOUD_VERSION },
                { 2L, 1015L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 25L, DATA_CLOUD_VERSION },
                { 9L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null, null,
                        26L, DATA_CLOUD_VERSION },
                { 14L, 176L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 27L, DATA_CLOUD_VERSION },
                { 18L, 226L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 28L, DATA_CLOUD_VERSION },
                { 1L, 147L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, DATA_CLOUD_VERSION },
                { 1L, 410L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, DATA_CLOUD_VERSION },
                { 1L, 612L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, DATA_CLOUD_VERSION },
                { 1L, 731L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, DATA_CLOUD_VERSION },
                { 1L, 847L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, DATA_CLOUD_VERSION },
                { 1L, 888L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, DATA_CLOUD_VERSION },
                { 1L, 899L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, DATA_CLOUD_VERSION },
                { 1L, 911L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, DATA_CLOUD_VERSION },
                { 1L, 986L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, DATA_CLOUD_VERSION },
                { 2L, 53L, 1057L, 1069L, 1080L, 1081L, 88L, "618@88|616@88|619@88|622@88|614@88|621@8",
                        "324@14|325@14|326@14|327@14|328@9|329@14", null, null, 10L, DATA_CLOUD_VERSION },
                { 2L, 425L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, DATA_CLOUD_VERSION },
                { 2L, 491L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, DATA_CLOUD_VERSION },
                { 2L, 730L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, DATA_CLOUD_VERSION },
                { 2L, 742L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, DATA_CLOUD_VERSION },
                { 2L, 820L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, DATA_CLOUD_VERSION },
                { 2L, 832L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, DATA_CLOUD_VERSION },
                { 2L, 837L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, DATA_CLOUD_VERSION },
                { 2L, 847L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 18L, DATA_CLOUD_VERSION },
                { 7L, 761L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, DATA_CLOUD_VERSION },
                { 8L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null, null,
                        20L, DATA_CLOUD_VERSION },
                { 10L, 53L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 21L, DATA_CLOUD_VERSION },
                { 12L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@0|623@", null, null,
                        null, 22L, DATA_CLOUD_VERSION },
                { 14L, 53L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 23L, DATA_CLOUD_VERSION },
                { 14L, 789L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 24L, DATA_CLOUD_VERSION },
                { 15L, 374L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 25L, DATA_CLOUD_VERSION } };
        return expectedData;
    }
}
