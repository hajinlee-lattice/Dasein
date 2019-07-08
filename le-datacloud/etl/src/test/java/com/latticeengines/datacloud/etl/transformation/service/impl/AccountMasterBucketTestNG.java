package com.latticeengines.datacloud.etl.transformation.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceBucketer;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceSorter;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class AccountMasterBucketTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterBucketTestNG.class);

    @Autowired
    protected AccountMaster accountMaster;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private Long expectedCount;

    private static final String DATA_CLOUD_VERSION = "2.0.6";

    @Test(groups = "functional")
    public void testTransformation() throws Exception {
        uploadBaseSourceFile(accountMaster.getSourceName(), "AMBucketTest_AM", baseSourceVersion);
        expectedCount = hdfsSourceEntityMgr.count(accountMaster, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        verifySort();
        verifyStats();
        verifyAvsc();
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return "AccountMasterBucketed";
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(accountMaster.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AccountMasterBucket");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig profile = profile();
            TransformationStepConfig sortProfile = sortProfile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calcStats = calcStats();
            TransformationStepConfig sortBucket = sortBucket();
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    profile, //
                    sortProfile, //
                    bucket, //
                    calcStats, //
                    sortBucket //
            );
            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
        int rowCount = 0;
        boolean hasNotZero = false;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            for (Schema.Field field : record.getSchema().getFields()) {
                if (!hasNotZero && field.name().startsWith("EAttr")) {
                    long value = (Long) record.get(field.pos());
                    if (value != 0) {
                        hasNotZero = true;
                    }
                }
            }
            rowCount++;
        }
        Assert.assertTrue(hasNotZero);
        Assert.assertEquals(rowCount, expectedCount.intValue());
    }

    protected TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = Collections.singletonList(accountMaster.getSourceName());
        step.setBaseSources(baseSources);
        step.setTransformer(SourceProfiler.TRANSFORMER_NAME);
        step.setConfiguration(JsonUtils.serialize(constructProfileConfig()));
        return step;
    }

    protected ProfileConfig constructProfileConfig() {
        ProfileConfig config = new ProfileConfig();
        config.setDataCloudVersion(DATA_CLOUD_VERSION);
        return config;
    }

    private TransformationStepConfig sortProfile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(0));
        step.setTransformer(SourceSorter.TRANSFORMER_NAME);
        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setSortingField(PROFILE_ATTR_ATTRNAME);
        conf.setCompressResult(true);
        step.setConfiguration(JsonUtils.serialize(conf));
        step.setTargetSource("AccountMasterProfile");
        return step;
    }

    protected TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList(accountMaster.getSourceName()));
        step.setInputSteps(Collections.singletonList(1));
        step.setTransformer(SourceBucketer.TRANSFORMER_NAME);
        step.setConfiguration("{}");
        return step;
    }

    protected TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(0, 2));
        step.setTransformer(CalculateStats.TRANSFORMER_NAME);
        step.setConfiguration("{}");
        step.setTargetSource("AccountMasterBucketedStats");
        return step;
    }

    private TransformationStepConfig sortBucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(2));
        step.setTransformer(SourceSorter.TRANSFORMER_NAME);
        SorterConfig config = new SorterConfig();
        config.setPartitions(100);
        config.setSortingField(InterfaceName.LatticeAccountId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        step.setTargetSource(getTargetSourceName());
        return step;
    }

    private void verifySort() throws IOException {
        String resultDir = getPathForResult();
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, resultDir + "/*.avro");
        long maxInLastFile = Long.MIN_VALUE;
        for (String file : files) {
            String fileName = file.substring(file.lastIndexOf("/") + 1);
            long minInFile = Long.MAX_VALUE;
            long maxInFile = Long.MIN_VALUE;
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, file);
            for (GenericRecord record : records) {
                long id = (long) record.get(InterfaceName.LatticeAccountId.name());
                minInFile = Math.min(id, minInFile);
                maxInFile = Math.max(id, maxInFile);
            }
            System.out.println(String.format("[%s] Min: %d -- Max: %d", fileName, minInFile, maxInFile));
            Assert.assertTrue(minInFile > maxInLastFile, String.format(
                    "Min in current file %d is not greater than the max in last file %d", minInFile, maxInLastFile));
            maxInLastFile = maxInFile;
        }
    }

    private void verifyAvsc() throws IOException {
        String avroDir = getPathForResult();
        String avscDir = avroDir.replace("Snapshot", "Schema");
        String avsc = HdfsUtils.getFilesByGlob(yarnConfiguration, avscDir + "/*.avsc").get(0);
        ObjectMapper om = new ObjectMapper();
        JsonNode json = om.readTree(HdfsUtils.getInputStream(yarnConfiguration, avsc));
        ArrayNode attrs = (ArrayNode) json.get("fields");
        for (JsonNode attr : attrs) {
            if (attr.get("name").asText().equals("EAttr001")) {
                System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(attr));
            }
            if (attr.get("name").asText().equals("EAttr100")) {
                System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(attr));
            }
        }
    }

    private void verifyStats() {
        Iterator<GenericRecord> records = iterateSource("AccountMasterBucketedStats");
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String attrName = record.get("AttrName").toString();
            if (attrName.startsWith("TechIndicator")) {
                String[] bkts = record.get("BktCounts").toString().split("\\|");
                for (String bkt : bkts) {
                    String[] tokens = bkt.split(":");
                    int bktId = Integer.valueOf(tokens[0]);
                    Assert.assertTrue(bktId >= 0 && bktId < 3, "Found an invalid bkt id " + bktId);
                }
            }
            long attrCount = (long) record.get(STATS_ATTR_COUNT);
            Assert.assertTrue(attrCount <= expectedCount);
            if (attrName.equals("OUT_OF_BUSINESS_INDICATOR")) {
                System.out.print(record);
            }
        }
    }
}
