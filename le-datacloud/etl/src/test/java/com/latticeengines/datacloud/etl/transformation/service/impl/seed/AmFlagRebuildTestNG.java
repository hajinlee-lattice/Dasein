package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateIndustryStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AmFlagRebuildTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AmFlagRebuildTestNG.class);

    private GeneralSource source = new GeneralSource("FinalOutput");
    private GeneralSource orbCompanyRaw = new GeneralSource("OrbCompanyRaw");
    private GeneralSource targetSource1 = new GeneralSource("TargetSource1");
    private GeneralSource targetSource3 = new GeneralSource("TargetSource3");
    private GeneralSource orbDomainRaw = new GeneralSource("OrbDomainRaw");

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(orbCompanyRaw, "OrbCompanyRaw", baseSourceVersion);
        uploadBaseSourceFile(orbDomainRaw, "OrbDomainRaw", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        configuration.setName("AmFlagRebuild");
        configuration.setVersion(targetVersion);

        //
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(orbCompanyRaw.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step1.setTargetSource(targetSource1.getSourceName());
        String confParamStr1 = getStdConfigForOrbCompanyMarker();
        step1.setConfiguration(confParamStr1);

        //
        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(targetSource1.getSourceName());
        step2.setBaseSources(baseSources);
        step2.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step2.setTargetSource(targetSource3.getSourceName());
        String confParamStr2 = getStdConfigForCleanup();
        step2.setConfiguration(confParamStr2);

        //
        TransformationStepConfig step3 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(orbDomainRaw.getSourceName());
        step3.setBaseSources(baseSources);
        step3.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step3.setTargetSource(source.getSourceName());
        String confParamStr3 = getStdConfigForOrbDomain();
        step3.setConfiguration(confParamStr3);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);

        // -----------
        configuration.setSteps(steps);
        configuration.setAMJob(true);

        return configuration;
    }

    private String getStdConfigForOrbCompanyMarker() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] domainFields = { "Website" };
        conf.setDomainFields(domainFields);
        String[] convertTypeFields = { "Employee", "LocationEmployee", "FacebookLikes",
                "TwitterFollowers", "TotalAmountRaised", "LastFundingRoundAmount", "SearchRank" };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = { //
                TypeConvertStrategy.STRING_TO_INT, TypeConvertStrategy.STRING_TO_INT, //
                TypeConvertStrategy.STRING_TO_LONG, TypeConvertStrategy.STRING_TO_LONG, //
                TypeConvertStrategy.STRING_TO_LONG, TypeConvertStrategy.STRING_TO_LONG, //
                TypeConvertStrategy.STRING_TO_LONG };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        String[] dedupFields = { "OrbNum" };
        conf.setDedupFields(dedupFields);
        String[] addConsolidatedRangeFields = { "ConsolidateEmployeeRange",
                "ConsolidateRevenueRange" };
        conf.setAddConsolidatedRangeFields(addConsolidatedRangeFields);
        ConsolidateRangeStrategy[] strategies = { ConsolidateRangeStrategy.MAP_VALUE,
                ConsolidateRangeStrategy.MAP_RANGE };
        conf.setConsolidateRangeStrategies(strategies);
        String[] rangeInputFields = { "Employee", "RevenueRange" };
        conf.setRangeInputFields(rangeInputFields);
        String[] rangeMapFileNames = { "EmployeeRangeMapping.txt", "OrbRevenueRangeMapping.txt" };
        conf.setRangeMapFileNames(rangeMapFileNames);
        conf.setConsolidateIndustryStrategy(ConsolidateIndustryStrategy.MAP_INDUSTRY);
        conf.setAddConsolidatedIndustryField("PrimaryIndustry");
        conf.setIndustryField("Industry");
        conf.setIndustryMapFileName("OrbIndustryMapping.txt");
        String markerExpression = "OrbNum != null && Website != null";
        conf.setMarkerExpression(markerExpression);
        String[] markerCheckFields = { "OrbNum", "Website" };
        conf.setMarkerCheckFields(markerCheckFields);
        String markerField = "IsValid";
        conf.setMarkerField(markerField);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { //
                StandardizationStrategy.DEDUP, StandardizationStrategy.DOMAIN, //
                StandardizationStrategy.CONVERT_TYPE, StandardizationStrategy.CONSOLIDATE_RANGE, //
                StandardizationStrategy.CONSOLIDATE_INDUSTRY, StandardizationStrategy.MARKER };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getStdConfigForCleanup() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String filterExpression = "IsValid == true";
        conf.setFilterExpression(filterExpression);
        String[] filterFields = { "IsValid" };
        conf.setFilterFields(filterFields);
        String[] discardFields = { "IsValid" };
        conf.setDiscardFields(discardFields);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = {
                StandardizationStrategy.FILTER, StandardizationStrategy.DISCARD };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getStdConfigForOrbDomain() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] dedupFields = { "OrbNum", "WebDomain" };
        conf.setDedupFields(dedupFields);
        String[] domainFields = { "WebDomain" };
        conf.setDomainFields(domainFields);
        String[] convertTypeFields = { "DomainHasEmail", "DomainHasWebsite",
                "DomainIsEmailHosting" };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = { //
                TypeConvertStrategy.STRING_TO_BOOLEAN, TypeConvertStrategy.STRING_TO_BOOLEAN,
                TypeConvertStrategy.STRING_TO_BOOLEAN };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        String markerExpression = "OrbNum != null && WebDomain != null";
        conf.setMarkerExpression(markerExpression);
        String[] markerCheckFields = { "OrbNum", "WebDomain" };
        conf.setMarkerCheckFields(markerCheckFields);
        String markerField = "IsValid";
        conf.setMarkerField(markerField);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { //
                StandardizationStrategy.DOMAIN, StandardizationStrategy.DEDUP, //
                StandardizationStrategy.CONVERT_TYPE, StandardizationStrategy.MARKER };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Completed Successfully");
    }
}
