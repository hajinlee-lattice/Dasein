package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.MergeTimeSeriesDeleteDataTxfmr;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.SoftDeleteTxfmr;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;

public class MergeTimeSeriesDeleteDataTxfmrTestNG extends PipelineTransformationTestNGBase {

    private final GeneralSource DeleteSrc = new GeneralSource("Delete");
    private final GeneralSource BaseSrc = new GeneralSource("Base");

    private static final Logger log = LoggerFactory.getLogger(MergeTimeSeriesDeleteDataTxfmrTestNG.class);

    @Test(groups = "functional")
    public void testTransformation() {
        prepareDeleteData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return "MergedTSDeleteData";
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("MergeTSDeleteData");
        configuration.setVersion(targetVersion);

        // add time range to delete input
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(DeleteSrc.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(MergeTimeSeriesDeleteDataTxfmr.TRANSFORMER_NAME);
        MergeTimeSeriesDeleteDataConfig config = new MergeTimeSeriesDeleteDataConfig();
        config.joinKey = InterfaceName.AccountId.name();
        config.numberOfDeleteInputs = 1;
        config.timeRanges.put(0, Arrays.asList(15L, 100L));
        step1.setConfiguration(JsonUtils.serialize(config));

        // delete transaction store
        TransformationStepConfig step2 = new TransformationStepConfig();
        List<String> srcToDelete = new ArrayList<>();
        srcToDelete.add(BaseSrc.getSourceName());
        step2.setInputSteps(Collections.singletonList(0));
        step2.setBaseSources(srcToDelete);
        step2.setTransformer(SoftDeleteTxfmr.TRANSFORMER_NAME);
        step2.setTargetSource(getTargetSourceName());
        SoftDeleteConfig jobConfig = new SoftDeleteConfig();
        jobConfig.setIdColumn(InterfaceName.AccountId.name());
        jobConfig.setDeleteSourceIdx(0);
        jobConfig.setTimeRangesColumn(InterfaceName.TimeRanges.name());
        jobConfig.setEventTimeColumn(InterfaceName.TransactionTime.name());
        step2.setConfiguration(JsonUtils.serialize(jobConfig));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        AtomicInteger cnt = new AtomicInteger();
        Map<String, Set<String>> expectedRecordNames = getExpectedRecordNames();
        Map<String, Set<String>> recordNames = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String accId = record.get(InterfaceName.AccountId.name()).toString();
            String name = record.get(InterfaceName.Name.name()).toString();
            log.info("AccountId = {}, TimeRanges = {}", record.get(InterfaceName.AccountId.name()), record.get(InterfaceName.TransactionTime.name()));
            cnt.incrementAndGet();
            recordNames.putIfAbsent(accId, new HashSet<>());
            recordNames.get(accId).add(name);
        }
        log.info("Total records = {}", cnt.get());
        Assert.assertEquals(recordNames, expectedRecordNames, "Retained (not deleted) records does not match the expected values");
    }

    // account id -> Name column of that record
    private Map<String, Set<String>> getExpectedRecordNames() {
        Map<String, Set<String>> expectedNames = new HashMap<>();
        expectedNames.put("A1", Sets.newHashSet("a", "b"));
        expectedNames.put("A2", Sets.newHashSet("e"));
        expectedNames.put("A5", Sets.newHashSet("h"));
        expectedNames.put("A6", Sets.newHashSet("i"));
        return expectedNames;
    }

    private void prepareDeleteData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        uploadBaseSourceData(DeleteSrc.getSourceName(), baseSourceVersion, columns, new Object[][] { //
                { "A1" }, //
                { "A2" }, //
                { "A5" }, //
                { "A100" }, //
        });

        List<Pair<String, Class<?>>> baseColumns = new ArrayList<>();
        baseColumns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        baseColumns.add(Pair.of(InterfaceName.TransactionTime.name(), Long.class));
        baseColumns.add(Pair.of(InterfaceName.Name.name(), String.class));
        baseColumns.add(Pair.of(InterfaceName.CompanyName.name(), String.class));
        uploadBaseSourceData(BaseSrc.getSourceName(), baseSourceVersion, baseColumns, new Object[][] { //
                { "A1", 1L, "a", "DnB" }, // out of range, not deleted
                { "A1", 10L, "b", "DnB" }, // out of range, not deleted
                { "A1", 15L, "c", "DnB" }, //
                { "A2", 100L, "d", "DnB" }, //
                { "A2", 14L, "e", "DnB" }, // out of range, not deleted
                { "A5", 50L, "f", "DnB" }, //
                { "A5", 55L, "g", "DnB" }, //
                { "A5", 101L, "h", "DnB" }, // out of range, not deleted
                { "A6", 100L, "i", "DnB" }, // not in ID list
        });
    }
}
