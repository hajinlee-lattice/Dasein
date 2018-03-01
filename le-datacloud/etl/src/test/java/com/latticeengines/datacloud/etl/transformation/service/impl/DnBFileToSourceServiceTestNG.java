package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DnBFileToSourceServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(DnBFileToSourceServiceTestNG.class);

    GeneralSource source = new GeneralSource("DnBCacheSeedRaw");

    @Autowired
    IngestionSource baseSource;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        baseSource.setIngestionName(IngestionNames.DNB_CASHESEED);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_052.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_053.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_054.OUT.gz", baseSourceVersion);
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
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("DnBFileToSource");
            configuration.setVersion(targetVersion);

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("IngestionSource");
            step1.setBaseSources(baseSources);
            step1.setTransformer("ingestedFileToSourceTransformer");
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getIngestedFileToSourceTransformerConfig();
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

    private String getIngestedFileToSourceTransformerConfig() throws JsonProcessingException {
        IngestedFileToSourceTransformerConfig conf = new IngestedFileToSourceTransformerConfig();
        conf.setIngestionName(IngestionNames.DNB_CASHESEED);
        conf.setFileNameOrExtension(".OUT");
        conf.setCompressedFileNameOrExtension(".OUT.gz");
        conf.setCompressType(CompressType.GZ);
        conf.setDelimiter("|");
        conf.setQualifier(null);
        conf.setCharset("ISO-8859-1");
        return om.writeValueAsString(conf);
    }

    @Override
    protected String getPathForResult() {
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        return hdfsPathBuilder.constructTransformationSourceDir(source, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[] expectedDuns = { "980565675", "980565683", "980565688", "980565691", "980565696", "980565626",
                "980565634", "980565639", "980565642", null };
        Set<String> set = new HashSet<>(Arrays.asList(expectedDuns));
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object duns = record.get("DUNS_NUMBER");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Assert.assertTrue(set.contains(duns));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 15);
    }
}
