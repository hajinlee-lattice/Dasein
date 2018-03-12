package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class MultiPeriodSupportTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(MiniAMDomainDunsTestNG.class);
    GeneralSource periodConverterInput = new GeneralSource("PeriodConverterInput");
    GeneralSource periodConverterWeekOutput = new GeneralSource(
            DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarWeek.getName());
    GeneralSource periodConverterMonthOutput = new GeneralSource(
            DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarMonth.getName());
    GeneralSource periodConverterQuarterOutput = new GeneralSource(
            DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarQuarter.getName());
    GeneralSource periodConverterYearOutput = new GeneralSource(
            DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarYear.getName());

    GeneralSource source = periodConverterYearOutput;

    String targetSourceName = source.getSourceName();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        preparePeriodConverterInput();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(periodConverterWeekOutput, null);
        confirmIntermediateSource(periodConverterMonthOutput, null);
        confirmIntermediateSource(periodConverterQuarterOutput, null);
        confirmIntermediateSource(periodConverterYearOutput, null);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
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
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("MultiPeriodSupport");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = addPeriod(PeriodStrategy.CalendarWeek);
        TransformationStepConfig step2 = addPeriod(PeriodStrategy.CalendarMonth);
        TransformationStepConfig step3 = addPeriod(PeriodStrategy.CalendarQuarter);
        TransformationStepConfig step4 = addPeriod(PeriodStrategy.CalendarYear);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private TransformationStepConfig addPeriod(PeriodStrategy periodStrategy) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        List<String> baseSources = new ArrayList<>();
        baseSources.add(periodConverterInput.getSourceName());
        step.setBaseSources(baseSources);
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategy(periodStrategy);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        step.setTargetSource(DataCloudConstants.PERIOD_CONVERTOR + periodStrategy.getName());
        return step;
    }

    private void preparePeriodConverterInput() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.TransactionDate.name(), String.class));
        Object[][] data = new Object[][] { { "A1", "2018-02-10" }, { "A2", "2017-02-10" } };
        uploadBaseSourceData(periodConverterInput.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify intermediate source %s", source));
        if (source.equals(DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarWeek.getName())) {
            verifyPeriodConverterWeekOutput(records);
        }
        if (source.equals(DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarMonth.getName())) {
            verifyPeriodConverterMonthOutput(records);
        }
        if (source.equals(DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarQuarter.getName())) {
            verifyPeriodConverterQuarterOutput(records);
        }
        if (source.equals(DataCloudConstants.PERIOD_CONVERTOR + PeriodStrategy.CalendarYear.getName())) {
            verifyPeriodConverterYearOutput(records);
        }
    }

    @SuppressWarnings("serial")
    private void verifyPeriodConverterWeekOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 945);
                put("A2", 893);
            }
        };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertEquals(record.get(InterfaceName.PeriodId.name()),
                    expected.get(record.get(InterfaceName.AccountId.name()).toString()));
        }
    }

    @SuppressWarnings("serial")
    private void verifyPeriodConverterMonthOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 217);
                put("A2", 205);
            }
        };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertEquals(record.get(InterfaceName.PeriodId.name()),
                    expected.get(record.get(InterfaceName.AccountId.name()).toString()));
        }
    }

    @SuppressWarnings("serial")
    private void verifyPeriodConverterQuarterOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 72);
                put("A2", 68);
            }
        };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertEquals(record.get(InterfaceName.PeriodId.name()),
                    expected.get(record.get(InterfaceName.AccountId.name()).toString()));
        }
    }

    @SuppressWarnings("serial")
    private void verifyPeriodConverterYearOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 18);
                put("A2", 17);
            }
        };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertEquals(record.get(InterfaceName.PeriodId.name()),
                    expected.get(record.get(InterfaceName.AccountId.name()).toString()));
        }
    }

}
