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
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class MultiPeriodSupportTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(MultiPeriodSupportTestNG.class);

    private static final String BUSINESS = "Business";

    GeneralSource periodConverterInput = new GeneralSource("PeriodConverterInput");
    GeneralSource weekOutput = new GeneralSource(PeriodStrategy.CalendarWeek.getName());
    GeneralSource monthOutput = new GeneralSource(PeriodStrategy.CalendarMonth.getName());
    GeneralSource quarterOutput = new GeneralSource(PeriodStrategy.CalendarQuarter.getName());
    GeneralSource yearOutput = new GeneralSource(PeriodStrategy.CalendarYear.getName());

    GeneralSource businessWeekOutput = new GeneralSource(BUSINESS + PeriodStrategy.CalendarWeek.getName());
    GeneralSource businessMonthOutput = new GeneralSource(BUSINESS + PeriodStrategy.CalendarMonth.getName());
    GeneralSource businessQuarterOutput = new GeneralSource(BUSINESS + PeriodStrategy.CalendarQuarter.getName());
    GeneralSource businessYearOutput = new GeneralSource(BUSINESS + PeriodStrategy.CalendarYear.getName());

    GeneralSource source = businessYearOutput;

    String targetSourceName = source.getSourceName();

    @Test(groups = "functional", enabled = false)
    public void testTransformation() {
        preparePeriodConverterInput();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(weekOutput, null);
        confirmIntermediateSource(monthOutput, null);
        confirmIntermediateSource(quarterOutput, null);
        confirmIntermediateSource(yearOutput, null);
        confirmIntermediateSource(businessWeekOutput, null);
        confirmIntermediateSource(businessMonthOutput, null);
        confirmIntermediateSource(businessQuarterOutput, null);
        confirmIntermediateSource(businessYearOutput, null);
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

        TransformationStepConfig step1 = addPeriod(PeriodStrategy.CalendarWeek, "");
        TransformationStepConfig step2 = addPeriod(PeriodStrategy.CalendarMonth, "");
        TransformationStepConfig step3 = addPeriod(PeriodStrategy.CalendarQuarter, "");
        TransformationStepConfig step4 = addPeriod(PeriodStrategy.CalendarYear, "");
        TransformationStepConfig step5 = addPeriod(businessWeekStrategy(), BUSINESS);
        TransformationStepConfig step6 = addPeriod(businessMonthStrategy(), BUSINESS);
        TransformationStepConfig step7 = addPeriod(businessQuarterStrategy(), BUSINESS);
        TransformationStepConfig step8 = addPeriod(businessYearStrategy(), BUSINESS);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        steps.add(step5);
        steps.add(step6);
        steps.add(step7);
        steps.add(step8);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private TransformationStepConfig addPeriod(PeriodStrategy periodStrategy, String prefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        List<String> baseSources = new ArrayList<>();
        baseSources.add(periodConverterInput.getSourceName());
        step.setBaseSources(baseSources);
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        step.setTargetSource(prefix + periodStrategy.getName());
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
        if (source.equals(PeriodStrategy.CalendarWeek.getName())) {
            verifyWeekOutput(records);
        }
        if (source.equals(PeriodStrategy.CalendarMonth.getName())) {
            verifyMonthOutput(records);
        }
        if (source.equals(PeriodStrategy.CalendarQuarter.getName())) {
            verifyQuarterOutput(records);
        }
        if (source.equals(PeriodStrategy.CalendarYear.getName())) {
            verifyYearOutput(records);
        }
        if (source.equals(BUSINESS + PeriodStrategy.CalendarWeek.getName())) {
            verifyBusinessWeekOutput(records);
        }
        if (source.equals(BUSINESS + PeriodStrategy.CalendarMonth.getName())) {
            verifyBusinessMonthOutput(records);
        }
        if (source.equals(BUSINESS + PeriodStrategy.CalendarQuarter.getName())) {
            verifyBusinessQuarterOutput(records);
        }
        if (source.equals(BUSINESS + PeriodStrategy.CalendarYear.getName())) {
            verifyBusinessYearOutput(records);
        }
    }

    @SuppressWarnings("serial")
    private void verifyWeekOutput(Iterator<GenericRecord> records) {
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
    private void verifyBusinessWeekOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 941);
                put("A2", 889);
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
    private void verifyMonthOutput(Iterator<GenericRecord> records) {
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
    private void verifyBusinessMonthOutput(Iterator<GenericRecord> records) {
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 205);
                put("A2", 193);
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
    private void verifyQuarterOutput(Iterator<GenericRecord> records) {
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
    private void verifyBusinessQuarterOutput(Iterator<GenericRecord> records) {
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
    private void verifyYearOutput(Iterator<GenericRecord> records) {
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
    
    @SuppressWarnings("serial")
    private void verifyBusinessYearOutput(Iterator<GenericRecord> records) {
        @SuppressWarnings("unused")
        Map<String, Integer> expected = new HashMap<String, Integer>() {
            {
                put("A1", 18);
                put("A2", 17);
            }
        };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            /*
            Assert.assertEquals(record.get(InterfaceName.PeriodId.name()),
                    expected.get(record.get(InterfaceName.AccountId.name()).toString()));
                    */
        }
    }

    private PeriodStrategy businessWeekStrategy() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("JAN-01");
        calendar.setLongerMonth(0);
        return new PeriodStrategy(calendar, PeriodStrategy.Template.Week);
    }

    private PeriodStrategy businessMonthStrategy() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("DEC-31");
        calendar.setLongerMonth(2);
        return new PeriodStrategy(calendar, PeriodStrategy.Template.Month);
    }

    private PeriodStrategy businessQuarterStrategy() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DAY);
        calendar.setStartingDay("1st-SUN-JAN");
        calendar.setLongerMonth(0);
        return new PeriodStrategy(calendar, PeriodStrategy.Template.Quarter);
    }

    private PeriodStrategy businessYearStrategy() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DAY);
        calendar.setStartingDay("1st-SUN-JAN");
        calendar.setLongerMonth(2);
        return new PeriodStrategy(calendar, PeriodStrategy.Template.Year);
    }

}
