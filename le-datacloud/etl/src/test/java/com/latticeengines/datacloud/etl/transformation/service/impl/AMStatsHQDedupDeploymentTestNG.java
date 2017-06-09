package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

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
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMStatsHQDedupDeploymentTestNG extends PipelineTransformationTestNGBase {

    private static final Log log = LogFactory.getLog(AMStatsHQDedupDeploymentTestNG.class);

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMaster baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterStatsHQDuns";
    String targetVersion = "2017-05-12_01-32-12_UTC";
    private String statsJsonFileName = "AccountMasterStatsHQDunsCube_Test.json";
    private String dataCloudVersion;

    @Override
    protected String getTargetSourceName() {
        return targetSourceName;
    }

    @PostConstruct
    private void postConstruct() {
        dataCloudVersion = versionEntityMgr.currentApprovedVersionAsString();
    }

    @Test(groups = "deployment", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, baseSource.getSourceName() + "_Test" + targetSourceName,
                "2017-05-12_01-32-12_UTC");

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
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        ObjectMapper om = new ObjectMapper();

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add("AccountMaster");
        step0.setBaseSources(baseSources);
        step0.setBaseVersions(null);
        step0.setTransformer("sourceDeduper");
        step0.setTargetSource("AccountMasterDeduped");
        String deduperConfig = getDeduperConfig();
        step0.setConfiguration(deduperConfig);

        ///////////////////

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<Integer> inputSteps1 = new ArrayList<Integer>();
        inputSteps1.add(0);
        step1.setInputSteps(inputSteps1);
        step1.setTargetSource("amStatsHQDuns");
        step1.setTransformer("amStatsHQDunsTransformer");
        AccountMasterStatisticsConfig confParam1 = getAccountMasterStatsParameters();
        String confParamStr1 = null;
        try {
            confParamStr1 = om.writeValueAsString(confParam1);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        step1.setConfiguration(confParamStr1);

        ///////////////////

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps2 = new ArrayList<Integer>();
        inputSteps2.add(1);
        step2.setInputSteps(inputSteps2);
        step2.setTargetSource("amStatsMinMax");
        step2.setTransformer("amStatsMinMaxTransformer");

        AccountMasterStatisticsConfig confParam2 = getAccountMasterStatsParameters();
        String confParamStr2 = null;
        try {
            confParamStr2 = om.writeValueAsString(confParam2);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step2.setConfiguration(confParamStr2);

        //////////////////

        TransformationStepConfig step3 = new TransformationStepConfig();
        List<Integer> inputSteps3 = new ArrayList<Integer>();
        inputSteps3.add(1);
        inputSteps3.add(2);
        step3.setInputSteps(inputSteps3);
        step3.setTargetSource("amStatsBucketedSource");
        step3.setTransformer("amStatsLeafSubstitutionTransformer");

        AccountMasterStatisticsConfig confParam3 = getAccountMasterStatsParameters();
        String confParamStr3 = null;
        try {
            confParamStr3 = om.writeValueAsString(confParam3);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step3.setConfiguration(confParamStr3);

        /////////////////

        TransformationStepConfig step4 = new TransformationStepConfig();
        List<Integer> inputSteps4 = new ArrayList<Integer>();
        inputSteps4.add(3);
        step4.setInputSteps(inputSteps4);
        step4.setTargetSource("amStatsLeafNode");
        step4.setTransformer("amStatsLeafNodeTransformer");

        AccountMasterStatisticsConfig confParam4 = getAccountMasterStatsParameters();
        String confParamStr4 = null;
        try {
            confParamStr4 = om.writeValueAsString(confParam4);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step4.setConfiguration(confParamStr4);

        //////////////////

        TransformationStepConfig step5 = new TransformationStepConfig();
        List<Integer> inputSteps5 = new ArrayList<Integer>();
        inputSteps5.add(4);
        step5.setInputSteps(inputSteps5);
        step5.setTargetSource("amStatsDimAggregateWithHQDuns");
        step5.setTransformer("amStatsDimAggregateWithHQDunsTransformer");

        AccountMasterStatisticsConfig confParam5 = getAccountMasterStatsParameters();
        String confParamStr5 = null;
        try {
            confParamStr5 = om.writeValueAsString(confParam5);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step5.setConfiguration(confParamStr5);

        //////////////////

        TransformationStepConfig step6 = new TransformationStepConfig();
        List<Integer> inputSteps6 = new ArrayList<Integer>();
        inputSteps6.add(5);
        step6.setInputSteps(inputSteps6);
        step6.setTargetSource("amStatsDimExpandMergeWithHQDuns");
        step6.setTransformer("amStatsDimExpandMergeWithHQDunsTransformer");

        AccountMasterStatisticsConfig confParam6 = getAccountMasterStatsParameters();
        String confParamStr6 = null;
        try {
            confParamStr6 = om.writeValueAsString(confParam6);
            confParamStr6 = setDataFlowEngine(confParamStr6, "TEZ");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        step6.setConfiguration(confParamStr6);

        //////////////////

        TransformationStepConfig step7 = new TransformationStepConfig();
        List<Integer> inputSteps7 = new ArrayList<Integer>();
        inputSteps7.add(6);
        step7.setInputSteps(inputSteps7);
        step7.setTargetSource(targetSourceName);
        step7.setTransformer("amStatsReportTransformer");

        AccountMasterStatisticsConfig confParam7 = getAccountMasterStatsParameters();
        String confParamStr7 = null;
        try {
            confParamStr7 = om.writeValueAsString(confParam7);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step7.setConfiguration(confParamStr7);

        //////////////////

        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        steps.add(step5);
        steps.add(step6);
        steps.add(step7);

        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));

        try {
            System.out.println(om.writeValueAsString(configuration));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
        param.setDataCloudVersion(dataCloudVersion);

        List<String> dimensions = new ArrayList<>();
        dimensions.add("Location");
        dimensions.add("Industry");
        param.setDimensions(dimensions);
        param.setNumericalBucketsRequired(true);

        List<String> specialColumns = new ArrayList<>();
        specialColumns.add(AccountMasterStatsParameters.HQ_DUNS);
        specialColumns.add(AccountMasterStatsParameters.DOMAIN_BCK_FIELD);
        param.setSpecialColumns(specialColumns);

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

        boolean verifyRecord = false;

        while (records.hasNext()) {
            GenericRecord record = records.next();

            boolean foundMatchingRecord = true;
            String encodedCubeStr = null;
            for (Object[] data : expectedData) {
                int idx = 0;
                isTopLocation = false;
                isTopIndustry = false;

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
                        if (verifyRecord) {
                            if ((val == null && expectedVal != null) //
                                    || (val != null && !val.equals(expectedVal))) {
                                if (val != null && val instanceof String
                                        && ((String) val).startsWith((String) expectedVal)) {
                                    // consider it matching field
                                } else {
                                    foundMatchingRecord = false;
                                    break;
                                }
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

                if (!foundMatchingRecord) {
                    break;
                }

                if (isTopIndustry && isTopLocation) {
                    if (encodedCubeStr != null && topmostCubeEncodedStr == null) {
                        topmostCubeEncodedStr = encodedCubeStr;
                        break;
                    }
                }

            }

            if (foundMatchingRecord) {
                System.out.println("\n\n================" + rowNum);
                for (Field field : record.getSchema().getFields()) {
                    if (!field.name().equals("EncodedCube")) {
                        System.out.print(", " + field.name() + ":");

                        if (record.get(field.name()) == null) {
                            System.out.print("null");
                        } else if (record.get(field.name()) instanceof Long) {
                            System.out.print(record.get(field.name()) + "L");
                        } else if (record.get(field.name()) instanceof Utf8) {
                            String txt = ((Utf8) record.get(field.name())).toString();
                            txt = txt.substring(0, (txt.length() < 40 ? txt.length() : 40));
                            System.out.print("\"" + txt + "\"");
                        } else {
                            throw new RuntimeException(record.get(field.name()).getClass().getName());
                        }
                    }
                }
                System.out.println("================\n");
                AccountMasterCube cube = null;
                try {
                    if (encodedCubeStr != null) {
                        cube = AMStatsUtils.decompressAndDecode(encodedCubeStr, AccountMasterCube.class);
                        printCube(cube);
                    } else {
                        System.out.println("Empty encoded cube");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("\n\n================" + rowNum);
            } 
            Assert.assertTrue(foundMatchingRecord);
            
            rowNum++;
        }

        System.out.println("Final rows " + rowNum);
        Assert.assertEquals(rowNum, 193);

        InputStream expectedCubeStream = ClassLoader.getSystemResourceAsStream("sources/" + statsJsonFileName);
        AccountMasterCube expectedCube = null;
        AccountMasterCube actualCube = null;

        ObjectMapper om = new ObjectMapper();
        try {
            actualCube = AMStatsUtils.decompressAndDecode(topmostCubeEncodedStr, AccountMasterCube.class);
            expectedCube = om.readValue(expectedCubeStream, AccountMasterCube.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        compareCubes(expectedCube, actualCube);
    }

    private void printCube(AccountMasterCube actualCube) {
        for (String attr : actualCube.getStatistics().keySet()) {
            if (!attr.equals("FeatureTermCellular")) {
                continue;
            }
            System.out.println("Attribute " + attr);
            AttributeStats actualRowBasedAttrStats = actualCube.getStatistics().get(attr)
                    .getRowBasedStatistics();
            if (actualRowBasedAttrStats.getNonNullCount() != 0) {
                System.out.println("Null count " + actualRowBasedAttrStats.getNonNullCount());
            }
            if (actualRowBasedAttrStats.getBuckets() != null) {
                System.out.println("Bucket type " + actualRowBasedAttrStats.getBuckets().getType());

                if (CollectionUtils.isNotEmpty(actualRowBasedAttrStats.getBuckets().getBucketList())) {
                    for (int i = 0; i < actualRowBasedAttrStats.getBuckets().getBucketList().size(); i++) {
                        Bucket actualBkt = actualRowBasedAttrStats.getBuckets().getBucketList().get(i);
                        printBucket(actualBkt);

                    }
                }
            }
        }
    }

    private void printBucket(Bucket actualBkt) {

        System.out.print("Bucket " + actualBkt.getBucketLabel() + " " + actualBkt.getCount() + " " + actualBkt.getId());
        if (actualBkt.getEncodedCountList() != null) {
            System.out.print("Encoded counts ");
            for (int i = 0; i < actualBkt.getEncodedCountList().length; i++) {
                System.out.print(actualBkt.getEncodedCountList()[i] + " ");
            }
            System.out.println("");
        }
        System.out.println("End bucket");
    }

    private void compareCubes(AccountMasterCube expectedCube, AccountMasterCube actualCube) {
        Assert.assertEquals(actualCube.getNonNullCount(), expectedCube.getNonNullCount());
        for (String attr : actualCube.getStatistics().keySet()) {
            System.out.println("Compare attribute " + attr);
            AttributeStats actualRowBasedAttrStats = actualCube.getStatistics().get(attr)
                    .getRowBasedStatistics();
            AttributeStats expectedRowBasedAttrStats = null;

            if (attr.equals(AccountMasterStatsParameters.HQ_DUNS) //
                    || attr.equals(AccountMasterStatsParameters.DOMAIN_BCK_FIELD)) {
                continue;
            }
            expectedRowBasedAttrStats = expectedCube.getStatistics().get(attr).getRowBasedStatistics();

            Assert.assertEquals(actualRowBasedAttrStats.getNonNullCount(), expectedRowBasedAttrStats.getNonNullCount());
            if (actualRowBasedAttrStats.getBuckets() != null) {
                Assert.assertNotNull(actualRowBasedAttrStats.getBuckets().getType());

                Assert.assertEquals(actualRowBasedAttrStats.getBuckets().getType(),
                        expectedRowBasedAttrStats.getBuckets().getType());

                System.out.println("Bucket type " + actualRowBasedAttrStats.getBuckets().getType() + "XXX");

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

        }
    }

    private void compareBuckets(Bucket expectedBkt, Bucket actualBkt) {

        Assert.assertEquals(actualBkt.getBucketLabel(), expectedBkt.getBucketLabel());
        Assert.assertEquals(actualBkt.getId(), expectedBkt.getId());

        if (expectedBkt.getEncodedCountList() != null) {
            Assert.assertEquals(actualBkt.getEncodedCountList().length, expectedBkt.getEncodedCountList().length);
            for (int i = 0; i < expectedBkt.getEncodedCountList().length; i++) {
                Assert.assertEquals(actualBkt.getEncodedCountList()[i], expectedBkt.getEncodedCountList()[i]);
            }
        } else {
            Assert.assertEquals(actualBkt.getCount(), expectedBkt.getCount());
        }
    }

    private Object[][] getExpectedRecordList() {
        Object[][] expectedData = {
                { 1L, 326L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 343L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 402L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 421L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 502L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 608L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 660L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 763L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 824L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 1L, 1012L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 1L, 1034L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 285L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 328L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 386L, 1057L, 1069L, 1080L, 1081L, 6L, "618@6|616@6|619@6|622@6|614@6|621@6|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 423L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 481L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 608L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 2L, 715L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 2L, 763L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 2L, 955L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, dataCloudVersion},
                { 2L, 1048L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, dataCloudVersion},
                { 8L, 905L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 22L, dataCloudVersion},
                { 15L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 23L, dataCloudVersion},
                { 18L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 24L, dataCloudVersion},
                { 1L, 423L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 491L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 618L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 665L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 733L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 832L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 837L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 840L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 867L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 1L, 884L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 1L, 905L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 222L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 343L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 410L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 621L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 719L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 789L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 2L, 805L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 2L, 825L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 2L, 867L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, dataCloudVersion},
                { 2L, 888L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, dataCloudVersion},
                { 18L, 176L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, dataCloudVersion},
                { 1L, 53L, 1057L, 1069L, 1080L, 1081L, 122L, "618@122|616@122|619@122|622@122|614@122|",
                        "314@16|315@16|316@16|317@16|318@16|319@1", null, null, 1L, dataCloudVersion},
                { 1L, 117L, 1057L, 1069L, 1080L, 1081L, 5L, "618@5|616@5|619@5|622@5|614@5|621@5|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 151L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 222L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 248L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 285L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 480L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 730L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 825L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 2L, 660L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 2L, 665L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 822L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 824L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 840L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 7L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null, null,
                        15L, dataCloudVersion},
                { 10L, 1034L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 11L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 12L, 783L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 14L, 222L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 15L, 612L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, dataCloudVersion},
                { 1L, 115L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 190L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 226L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 232L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 257L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 541L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 613L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 742L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 761L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 1L, 789L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 1L, 820L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 1L, 1015L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 147L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 259L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 462L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 502L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 534L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 2L, 750L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 2L, 979L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 2L, 986L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, dataCloudVersion},
                { 2L, 1012L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, dataCloudVersion},
                { 10L, 556L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, dataCloudVersion},
                { 1L, 120L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 273L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 386L, 1057L, 1069L, 1080L, 1081L, 7L, "618@7|616@7|619@7|622@7|614@7|621@7|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 414L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 425L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 556L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 955L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 990L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 1048L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 2L, 232L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 2L, 257L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 273L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 326L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 421L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 427L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 556L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 911L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 7L, 758L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 1L, 259L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 333L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 427L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 679L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 715L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 719L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 750L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 783L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 2L, 117L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 2L, 120L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 2L, 190L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 374L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 480L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 541L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 625L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 9L, 733L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 11L, 151L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 12L, 693L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 14L, 414L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 1L, 176L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 328L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 374L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 455L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 462L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 481L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 534L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 621L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 625L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 1L, 693L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null,
                        null, 10L, dataCloudVersion},
                { 1L, 703L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 1L, 758L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 1L, 805L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 1L, 822L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 1L, 979L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 333L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 402L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 2L, 455L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 2L, 613L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 2L, 679L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 20L, dataCloudVersion},
                { 2L, 703L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 21L, dataCloudVersion},
                { 2L, 731L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 22L, dataCloudVersion},
                { 2L, 733L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 23L, dataCloudVersion},
                { 2L, 990L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 24L, dataCloudVersion},
                { 2L, 1015L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 25L, dataCloudVersion},
                { 9L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null, null,
                        26L, dataCloudVersion},
                { 14L, 176L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 27L, dataCloudVersion},
                { 18L, 226L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 28L, dataCloudVersion},
                { 1L, 147L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 1L, dataCloudVersion},
                { 1L, 410L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 2L, dataCloudVersion},
                { 1L, 612L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 3L, dataCloudVersion},
                { 1L, 731L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 4L, dataCloudVersion},
                { 1L, 847L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 5L, dataCloudVersion},
                { 1L, 888L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 6L, dataCloudVersion},
                { 1L, 899L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 7L, dataCloudVersion},
                { 1L, 911L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 8L, dataCloudVersion},
                { 1L, 986L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 9L, dataCloudVersion},
                { 2L, 53L, 1057L, 1069L, 1080L, 1081L, 88L, "618@88|616@88|619@88|622@88|614@88|621@8",
                        "324@14|325@14|326@14|327@14|328@9|329@14", null, null, 10L, dataCloudVersion},
                { 2L, 425L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 11L, dataCloudVersion},
                { 2L, 491L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 12L, dataCloudVersion},
                { 2L, 730L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 13L, dataCloudVersion},
                { 2L, 742L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 14L, dataCloudVersion},
                { 2L, 820L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 15L, dataCloudVersion},
                { 2L, 832L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 16L, dataCloudVersion},
                { 2L, 837L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 17L, dataCloudVersion},
                { 2L, 847L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@2|623@", null, null,
                        null, 18L, dataCloudVersion},
                { 7L, 761L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 19L, dataCloudVersion},
                { 8L, 53L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@0|623@", null, null, null,
                        20L, dataCloudVersion},
                { 10L, 53L, 1057L, 1069L, 1080L, 1081L, 3L, "618@3|616@3|619@3|622@3|614@3|621@3|623@", null, null,
                        null, 21L, dataCloudVersion},
                { 12L, 53L, 1057L, 1069L, 1080L, 1081L, 2L, "618@2|616@2|619@2|622@2|614@2|621@0|623@", null, null,
                        null, 22L, dataCloudVersion},
                { 14L, 53L, 1057L, 1069L, 1080L, 1081L, 4L, "618@4|616@4|619@4|622@4|614@4|621@4|623@", null, null,
                        null, 23L, dataCloudVersion},
                { 14L, 789L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 24L, dataCloudVersion},
                { 15L, 374L, 1057L, 1069L, 1080L, 1081L, 1L, "618@1|616@1|619@1|622@1|614@1|621@1|623@", null, null,
                        null, 25L, dataCloudVersion} };
        return expectedData;
    }
}
