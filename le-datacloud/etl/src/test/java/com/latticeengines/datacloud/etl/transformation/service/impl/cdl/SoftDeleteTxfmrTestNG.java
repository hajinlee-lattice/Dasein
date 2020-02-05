package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.SoftDeleteTxfmr;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;

public class SoftDeleteTxfmrTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteTxfmrTestNG.class);

    private final GeneralSource DeleteSrc = new GeneralSource("Delete");
    private final GeneralSource BaseSrc = new GeneralSource("Base");

    private final String DeleteJoinId = "AccountId";

    @Override
    protected String getTargetSourceName() {
        return "Offspring";
    }

    @Test(groups = "functional")
    public void testTransformation() throws IOException {
        prepareBaseSrc();
        prepareDeleteSrc();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile();
        cleanupProgressTables();
    }

    private void confirmResultFile() throws IOException {
        String path = getPathForResult();
        List<String> allFiles = HdfsUtils.getFilesForDir(yarnConfiguration, path);
        allFiles = allFiles.stream().filter(file -> {
            try {
                return HdfsUtils.isDirectory(yarnConfiguration, file);
            } catch (IOException e) {
                log.error("Failed to get file info from HDFS.", e);
            }
            return false;
        }).collect(Collectors.toList());
        allFiles.forEach(file -> Assert.assertTrue(file.endsWith("Company=DnB")));
        allFiles = allFiles.stream().map(s -> s = s + "/*.avro").collect(Collectors.toList());
        List<String> subPaths = new ArrayList<>(allFiles);
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, subPaths);
        verifyResultAvroRecords(records.iterator());
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DnBSoftDelete");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(DeleteSrc.getSourceName());
        baseSources.add(BaseSrc.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(SoftDeleteTxfmr.TRANSFORMER_NAME);
        step1.setTargetSource(getTargetSourceName());
        SoftDeleteConfig jobConfig = new SoftDeleteConfig();
        jobConfig.setIdColumn(DeleteJoinId);
        jobConfig.setDeleteSourceIdx(0);
        jobConfig.setPartitionKeys(ImmutableList.of("Company"));
        jobConfig.setNeedPartitionOutput(Boolean.TRUE);
        step1.setConfiguration(JsonUtils.serialize(jobConfig));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int count = 0;
        Set<String> remainingContactIds = new HashSet<>(Arrays.asList("Contact5", "Contact6", "Contact7", "Contact9"));
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertEquals(record.getSchema().getFields().size(), 3);
            Assert.assertNotEquals(record.get(DeleteJoinId).toString(), "Account1");
            Assert.assertNotEquals(record.get(DeleteJoinId).toString(), "Account2");
            Assert.assertNotEquals(record.get(DeleteJoinId).toString(), "Account5");
            Assert.assertNotEquals(record.get(DeleteJoinId).toString(), "Account100");
            Assert.assertTrue(remainingContactIds.remove(record.get("ContactId").toString()));
            count++;
        }
        Assert.assertEquals(count, 4);
        Assert.assertEquals(remainingContactIds.size(), 0);
    }

    private void prepareDeleteSrc() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(DeleteJoinId, String.class));
        uploadBaseSourceData(DeleteSrc.getSourceName(), baseSourceVersion, columns, new Object[][] { //
                { "Account1"}, //
                { "Account2"}, //
                { "Account5"}, //
                { "Account100"}, //
        });
    }

    private void prepareBaseSrc() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ContactId", String.class));
        columns.add(Pair.of(DeleteJoinId, String.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("Company", String.class));
        uploadBaseSourceData(BaseSrc.getSourceName(), baseSourceVersion, columns, new Object[][] { //
                { "Contact1", "Account1", "a", "DnB" }, //
                { "Contact2", "Account1", "b", "DnB" }, //
                { "Contact3", "Account2", "c", "DnB" }, //
                { "Contact4", "Account2", "d", "DnB" }, //
                { "Contact5", "Account3", "e", "DnB" }, //
                { "Contact6", "Account3", "f", "DnB" }, //
                { "Contact7", "Account4", "g", "DnB" }, //
                { "Contact8", "Account5", "h", "DnB" }, //
                { "Contact9", "Account6", "i", "DnB" }, //
        });
    }
}
