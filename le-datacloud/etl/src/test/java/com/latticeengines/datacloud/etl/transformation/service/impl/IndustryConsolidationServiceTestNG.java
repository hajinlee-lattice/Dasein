package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.ConsolidateIndustryStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class IndustryConsolidationServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(IndustryConsolidationServiceTestNG.class);

    GeneralSource source = new GeneralSource("ConsolidateIndustry");

    GeneralSource baseSource = new GeneralSource("ConsolidateIndustry_Test");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "ConsolidateIndustry";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "ConsolidateIndustry_Test", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("IndustryConsolidation");
            configuration.setVersion(targetVersion);

            // Map industry
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("ConsolidateIndustry_Test");
            step1.setBaseSources(baseSources);
            step1.setTransformer("standardizationTransformer");
            String confParamStr1 = getIndustryMappingConfig();
            step1.setConfiguration(confParamStr1);

            // Parse Naics
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTransformer("standardizationTransformer");
            step2.setTargetSource(targetSourceName);
            String confParamStr2 = getNaicsMappingConfig();
            step2.setConfiguration(confParamStr2);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getIndustryMappingConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        conf.setConsolidateIndustryStrategy(ConsolidateIndustryStrategy.MAP_INDUSTRY);
        conf.setAddConsolidatedIndustryField("ConsolidatedIndustryFromIndustry");
        conf.setIndustryField("Industry");
        conf.setIndustryMapFileName("OrbIndustryMapping.txt");
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = {
                StandardizationStrategy.CONSOLIDATE_INDUSTRY };
        conf.setSequence(sequence);
        return om.writeValueAsString(conf);
    }

    private String getNaicsMappingConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        conf.setConsolidateIndustryStrategy(ConsolidateIndustryStrategy.PARSE_NAICS);
        conf.setAddConsolidatedIndustryField("ConsolidatedIndustryFromNaics");
        conf.setNaicsField("Naics");
        conf.setNaicsMapFileName("NaicsIndustryMapping.txt");
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = {
                StandardizationStrategy.CONSOLIDATE_INDUSTRY };
        conf.setSequence(sequence);
        return om.writeValueAsString(conf);
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
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @SuppressWarnings("serial")
    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<String, String> expectedIndustryMap = new HashMap<String, String>() {
            {
                put("Accounting", "Accounting");
                put("Aviation & Aerospace", "Defense, Aviation & Aeorospace");
                put("Business Supplies and Equipment", "Financial Services");
                put("Electrical/Electronic Manufacturing", "Manufacturing - Computer and Electronic");
                put("null", "null");
            }
        };
        Map<String, String> expectedNaicsMap = new HashMap<String, String>() {
            {
                put("561613", "Business Services");
                put("238220", "Construction");
                put("811111", "Consumer Services");
                put("424990", "Wholesale");
                put("null", "null");
            }
        };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String industry = String.valueOf(record.get("Industry"));
            String naics = String.valueOf(record.get("Naics"));
            String consolidatedIndustryFromIndustry = String.valueOf(record.get("ConsolidatedIndustryFromIndustry"));
            String consolidatedIndustryFromNaics = String.valueOf(record.get("ConsolidatedIndustryFromNaics"));
            Assert.assertEquals(expectedIndustryMap.get(industry), consolidatedIndustryFromIndustry);
            Assert.assertEquals(expectedNaicsMap.get(naics), consolidatedIndustryFromNaics);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 5);
    }
}
