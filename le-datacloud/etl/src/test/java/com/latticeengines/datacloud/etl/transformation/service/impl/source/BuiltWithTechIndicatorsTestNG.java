package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.BuiltWithTechIndicatorsFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TechIndicatorsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class BuiltWithTechIndicatorsTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuiltWithTechIndicatorsTestNG.class);

    private final String TECH_INDICATORS = "TechIndicators";

    private final String TECH_SEO_TITLE = "TechIndicator_SEO_TITLE";
    private final String TECH_MOD_SSL = "TechIndicator_mod_ssl";

    private int HAS_TECH_SEO_TITLE_POS = -1;
    private int HAS_MOD_SSL_POS = -1;

    private final ObjectMapper objectMapper = new ObjectMapper();

    GeneralSource source = new GeneralSource("BuiltWithTechIndicators");
    GeneralSource baseSource = new GeneralSource("BuiltWithMostRecent");

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional")
    public void testTransformation() {
        readBitPositions();
        uploadBaseAvro(baseSource, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
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
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("BuiltWithTechIndicators");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(BuiltWithTechIndicatorsFlow.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfig() throws JsonProcessingException {
        TechIndicatorsConfig config = new TechIndicatorsConfig();
        String[] groupByFields = { "Domain" };
        config.setGroupByFields(groupByFields);
        config.setTimestampField("Timestamp");
        return om.writeValueAsString(config);
    }

    @Override
    public void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        System.out.println("HAS_TECH_SEO_TITLE_POS: " + HAS_TECH_SEO_TITLE_POS);
        System.out.println("HAS_MOD_SSL_POS: " + HAS_MOD_SSL_POS);
        int recordsToCheck = 100;
        int pos = 0;
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            String domain = record.get("Domain").toString();
            try {
                boolean[] bits = BitCodecUtils.decode(record.get(TECH_INDICATORS).toString(),
                        new int[] { HAS_TECH_SEO_TITLE_POS, HAS_MOD_SSL_POS });
                if ("sandisland.com".equals(domain)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertTrue(bits[1]);
                }
            } catch (IOException e) {
                System.out.println(record);
                throw new RuntimeException(e);
            }
        }
    }

    private void readBitPositions() {
        List<SourceColumn> columns = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        for (SourceColumn column : columns) {
            String columnName = column.getColumnName();
            if (TECH_SEO_TITLE.equals(columnName)) {
                HAS_TECH_SEO_TITLE_POS = parseBitPos(column.getArguments());
            } else if (TECH_MOD_SSL.equals(columnName)) {
                HAS_MOD_SSL_POS = parseBitPos(column.getArguments());
            }
            if (Collections.min(Arrays.asList(HAS_TECH_SEO_TITLE_POS, HAS_MOD_SSL_POS)) > -1) {
                break;
            }
        }
    }

    private int parseBitPos(String arguments) {
        try {
            JsonNode jsonNode = objectMapper.readTree(arguments);
            return jsonNode.get("BitPosition").asInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
